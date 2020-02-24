import {debug, warning, info} from '@actions/core'
import {HttpClientResponse, HttpClient} from '@actions/http-client/index'
import {IHttpClientResponse} from '@actions/http-client/interfaces'
import {
  ArtifactResponse,
  CreateArtifactParameters,
  PatchArtifactSize,
  UploadResults
} from './internal-contracts'
import * as fs from 'fs'
import {UploadSpecification} from './internal-upload-specification'
import {UploadOptions} from './internal-upload-options'
import {URL} from 'url'
import {
  createHttpClient,
  getArtifactUrl,
  getContentRange,
  getRequestOptions,
  getRequestOptions2,
  isRetryableStatusCode,
  isSuccessStatusCode,
  checkArtifactFilePath
} from './internal-utils'
import {
  getUploadChunkConcurrency,
  getUploadChunkSize,
  getUploadFileConcurrency,
  getUploadRetryCount,
  getRetryWaitTime
} from './internal-config-variables'
import { performance } from 'perf_hooks'

/**
 * Creates a file container for the new artifact in the remote blob storage/file service
 * @param {string} artifactName Name of the artifact being created
 * @returns The response from the Artifact Service if the file container was successfully created
 */
export async function createArtifactInFileContainer(
  artifactName: string
): Promise<ArtifactResponse> {
  const parameters: CreateArtifactParameters = {
    Type: 'actions_storage',
    Name: artifactName
  }
  const data: string = JSON.stringify(parameters, null, 2)
  const artifactUrl = getArtifactUrl()
  const client = createHttpClient()
  const requestOptions = getRequestOptions('application/json')

  const rawResponse = await client.post(artifactUrl, data, requestOptions)
  const body: string = await rawResponse.readBody()

  if (isSuccessStatusCode(rawResponse.message.statusCode) && body) {
    return JSON.parse(body)
  } else {
    // eslint-disable-next-line no-console
    console.log(rawResponse)
    throw new Error(
      `Unable to create a container for the artifact ${artifactName}`
    )
  }
  client.dispose()
}

/**
 * Concurrently upload all of the files in chunks
 * @param {string} uploadUrl Base Url for the artifact that was created
 * @param {SearchResult[]} filesToUpload A list of information about the files being uploaded
 * @returns The size of all the files uploaded in bytes
 */
export async function uploadArtifactToFileContainer(
  uploadUrl: string,
  filesToUpload: UploadSpecification[],
  options?: UploadOptions
): Promise<UploadResults> {
  const client = createHttpClient()
  const FILE_CONCURRENCY = getUploadFileConcurrency()
  const CHUNK_CONCURRENCY = getUploadChunkConcurrency()
  const MAX_CHUNK_SIZE = getUploadChunkSize()
  debug(
    `File Concurrency: ${FILE_CONCURRENCY}, Chunk Concurrency: ${CHUNK_CONCURRENCY} and Chunk Size: ${MAX_CHUNK_SIZE}`
  )

  const parameters: UploadFileParameters[] = []

  // by default, file uploads will continue if there is an error unless specified differently in the options
  let continueOnError = true
  if (options) {
    if (options.continueOnError === false) {
      continueOnError = false
    }
  }

  // Prepare the necessary parameters to upload all the files
  for (const file of filesToUpload) {
    const resourceUrl = new URL(uploadUrl)
    resourceUrl.searchParams.append('itemPath', file.uploadFilePath)
    resourceUrl.searchParams.append('scope', '00000000-0000-0000-0000-000000000000')
    parameters.push({
      file: file.absoluteFilePath,
      resourceUrl: resourceUrl.toString(),
      restClient: client,
      concurrency: CHUNK_CONCURRENCY,
      maxChunkSize: MAX_CHUNK_SIZE,
      continueOnError
    })
  }

  const parallelUploads = [...new Array(FILE_CONCURRENCY).keys()]
  const failedItemsToReport: string[] = []
  let uploadedFiles = 0
  let fileSizes = 0
  let abortPendingFileUploads = false

  var t0 = performance.now();

  // Only allow a certain amount of files to be uploaded at once, this is done to reduce potential errors
  await Promise.all(
    parallelUploads.map(async () => {
      while (uploadedFiles < filesToUpload.length) {
        const currentFileParameters = parameters[uploadedFiles]
        var t5 = performance.now();
        console.log(`Starting upload for ${currentFileParameters.file}, time is ${(t5-t0)/1000}`)

        uploadedFiles += 1
        info(`File: ${uploadedFiles}/${filesToUpload.length}. ${currentFileParameters.file}`)
        if (abortPendingFileUploads) {
          failedItemsToReport.push(currentFileParameters.file)
          continue
        }

        if(uploadedFiles % 1000 === 0){
          var t1 = performance.now();
          console.log("Call to upload " + uploadedFiles + " took " + (t1 - t0)/1000 + " seconds.");
        }
        var t3 = performance.now();
        const uploadFileResult = await uploadFileAsync(currentFileParameters)
        var t4 = performance.now();
        console.log("Call to upload " + currentFileParameters.file + " took " + (t4 - t3) + " milliseconds.");

        fileSizes += uploadFileResult.successfulUploadSize
        if (uploadFileResult.isSuccess === false) {
          failedItemsToReport.push(currentFileParameters.file)
          if (!continueOnError) {
            // Existing uploads will be able to finish however all pending uploads will fail fast
            abortPendingFileUploads = true
          }
        }
      }
    })
  )

  info(`Total size of all the files uploaded is ${fileSizes} bytes`)
  return {
    size: fileSizes,
    failedItems: failedItemsToReport
  }
}

/**
 * Asynchronously uploads a file. If the file is bigger than the max chunk size it will be uploaded via multiple calls
 * @param {UploadFileParameters} parameters Information about the file that needs to be uploaded
 * @returns The size of the file that was uploaded in bytes along with any failed uploads
 */
async function uploadFileAsync(
  parameters: UploadFileParameters
): Promise<UploadFileResult> {
  const fileSize: number = fs.statSync(parameters.file).size
  const parallelUploads = [...new Array(parameters.concurrency).keys()]
  let offset = 0
  let isUploadSuccessful = true
  let failedChunkSizes = 0
  let abortFileUpload = false

  const newClient = createHttpClient()
  await Promise.all(
    parallelUploads.map(async () => {
      while (offset < fileSize) {
        const chunkSize = Math.min(fileSize - offset, parameters.maxChunkSize)
        if (abortFileUpload) {
          // if we don't want to continue on error, any pending upload chunk will be marked as failed
          failedChunkSizes += chunkSize
          continue
        }

        const start = offset
        const end = offset + chunkSize - 1
        offset += parameters.maxChunkSize
      
        //checkArtifactFilePath(parameters.file)
        const chunk: NodeJS.ReadableStream = fs.createReadStream(
          parameters.file,
          {
            start,
            end,
            autoClose: true
          }
        )

        
        const result = await uploadChunk(
          newClient,
          parameters.resourceUrl,
          chunk,
          start,
          end,
          fileSize
        )

        if (!result) {
          /**
           * Chunk failed to upload, report as failed and do not continue uploading any more chunks for the file. It is possible that part of a chunk was
           * successfully uploaded so the server may report a different size for what was uploaded
           **/
          isUploadSuccessful = false
          failedChunkSizes += chunkSize
          warning(`Aborting upload for ${parameters.file} due to failure`)
          abortFileUpload = true
        }
      }
    })
  )
  newClient.dispose()
  return {
    isSuccess: isUploadSuccessful,
    successfulUploadSize: fileSize - failedChunkSizes
  }
}

/**
 * Uploads a chunk of an individual file to the specified resourceUrl. If the upload fails and the status code
 * indicates a retryable status, we try to upload the chunk as well
 * @param {HttpClient} restClient RestClient that will be making the appropriate HTTP call
 * @param {string} resourceUrl Url of the resource that the chunk will be uploaded to
 * @param {NodeJS.ReadableStream} data Stream of the file that will be uploaded
 * @param {number} start Starting byte index of file that the chunk belongs to
 * @param {number} end Ending byte index of file that the chunk belongs to
 * @param {number} totalSize Total size of the file in bytes that is being uploaded
 * @returns if the chunk was successfully uploaded
 */
async function uploadChunk(
  restClient: HttpClient,
  resourceUrl: string,
  data: NodeJS.ReadableStream,
  start: number,
  end: number,
  totalSize: number
): Promise<boolean> {
  const requestOptions = getRequestOptions2(
    'application/octet-stream',
    totalSize,
    getContentRange(start, end, totalSize)
  )

  const uploadChunkRequest = async (): Promise<IHttpClientResponse> => {
    return await restClient.sendStream('PUT', resourceUrl, data, requestOptions)
  }

  let retryCount = 0
  const retryLimit = getUploadRetryCount()

  // Allow for failed chunks to be retried multiple times
  // change this to a nice for with retryCount incrementing
  while (retryCount <= retryLimit) {
    try{
      const response = await uploadChunkRequest()
      if (isSuccessStatusCode(response.message.statusCode)) {
        return true
      } else if (isRetryableStatusCode(response.message.statusCode)) {
        retryCount++
        if (retryCount > retryLimit) {
          info(
            `Retry limit has been reached for chunk at offset ${start} to ${resourceUrl}`
          )
          return false
        } else {
          info(
            `Received http ${response.message.statusCode} during chunk upload, will retry at offset ${start} after 10 seconds. Retry count #${retryCount}`
          )
          await new Promise(resolve => setTimeout(resolve, getRetryWaitTime()))
        }
      } else {
        info('Unable to upload chunk')
        // eslint-disable-next-line no-console
        console.log(response)
        return false
      }
    }
    catch(error){
      console.log(error)
      
      retryCount++
      if (retryCount > retryLimit) {
        info(
          `Retry limit has been reached for chunk at offset ${start} to ${resourceUrl}`
        )
        return false
      } else {
        info(
          `Retrying chunk upload after encountering an error`
        )
        await new Promise(resolve => setTimeout(resolve, getRetryWaitTime()))
      }
    }
  }
  return false
}

/**
 * Updates the size of the artifact from -1 which was initially set when the container was first created for the artifact.
 * Updating the size indicates that we are done uploading all the contents of the artifact. A server side check will be run
 * to check that the artifact size is correct for billing purposes
 */
export async function patchArtifactSize(
  size: number,
  artifactName: string
): Promise<void> {
  const client = createHttpClient()
  const requestOptions = getRequestOptions('application/json')
  const resourceUrl = new URL(getArtifactUrl())
  resourceUrl.searchParams.append('artifactName', artifactName)

  const parameters: PatchArtifactSize = {Size: size}
  const data: string = JSON.stringify(parameters, null, 2)
  debug(`URL is ${resourceUrl.toString()}`)

  const rawResponse: HttpClientResponse = await client.patch(
    resourceUrl.toString(),
    data,
    requestOptions
  )
  const body: string = await rawResponse.readBody()

  if (isSuccessStatusCode(rawResponse.message.statusCode)) {
    debug(
      `Artifact ${artifactName} has been successfully uploaded, total size ${size}`
    )
    debug(body)
  } else if (rawResponse.message.statusCode === 404) {
    throw new Error(`An Artifact with the name ${artifactName} was not found`)
  } else {
    // eslint-disable-next-line no-console
    console.log(body)
    throw new Error(`Unable to finish uploading artifact ${artifactName}`)
  }
  client.dispose()
}

interface UploadFileParameters {
  file: string
  resourceUrl: string
  restClient: HttpClient
  concurrency: number
  maxChunkSize: number
  continueOnError: boolean
}

interface UploadFileResult {
  isSuccess: boolean
  successfulUploadSize: number
}
