import * as fs from 'fs'
import * as zlib from 'zlib'
import * as tmp from 'tmp'
import {debug, warning, info} from '@actions/core'
import {HttpClientResponse, HttpClient} from '@actions/http-client/index'
import {IHttpClientResponse} from '@actions/http-client/interfaces'
import {
  ArtifactResponse,
  CreateArtifactParameters,
  PatchArtifactSize,
  UploadResults
} from './internal-contracts'
import {UploadSpecification} from './internal-upload-specification'
import {UploadOptions} from './internal-upload-options'
import {URL} from 'url'
import {
  createHttpClient,
  getArtifactUrl,
  getContentRange,
  getRequestOptions,
  isRetryableStatusCode,
  isSuccessStatusCode
} from './internal-utils'
import {
  getUploadChunkSize,
  getUploadFileConcurrency,
  getUploadRetryCount,
  getRetryWaitTime
} from './internal-config-variables'
import {performance} from 'perf_hooks'

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
  // No concurrent calls so a single client without `keep-alive` is sufficient
  const client = createHttpClient()
  const requestOptions = getRequestOptions('application/json', false, false)

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
  const FILE_CONCURRENCY = getUploadFileConcurrency()
  const MAX_CHUNK_SIZE = getUploadChunkSize()
  debug(
    `File Concurrency: ${FILE_CONCURRENCY}, and Chunk Size: ${MAX_CHUNK_SIZE}`
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
    parameters.push({
      file: file.absoluteFilePath,
      resourceUrl: resourceUrl.toString(),
      maxChunkSize: MAX_CHUNK_SIZE,
      continueOnError
    })
  }

  const parallelUploads = [...new Array(FILE_CONCURRENCY).keys()]
  // each parallel upload gets its own http client
  const clientManagerInstance = HttpUploadManager.getInstance()
  clientManagerInstance.createClients(FILE_CONCURRENCY)
  const failedItemsToReport: string[] = []
  let currentFile = 0
  let completedFiles = 0
  let fileSizes = 0
  let abortPendingFileUploads = false

  // Every 10 seconds, display information about the upload status
  const totalUploadStatus = setInterval(function() {
    info(
      `Total file(s): ${
        filesToUpload.length
      } ---- Processing file #${currentFile} (${(
        (completedFiles / filesToUpload.length) *
        100
      ).toFixed(2)}%)`
    )
  }, 10000)

  // Only allow a certain amount of files to be uploaded at once, this is done to reduce potential errors
  await Promise.all(
    parallelUploads.map(async index => {
      while (currentFile < filesToUpload.length) {
        const currentFileParameters = parameters[currentFile]
        currentFile += 1
        if (abortPendingFileUploads) {
          failedItemsToReport.push(currentFileParameters.file)
          continue
        }

        const startTime = performance.now()
        const uploadFileResult = await uploadFileAsync(
          index,
          currentFileParameters
        )

        if(uploadFileResult){
          // increment count after an uploadResult is returned
          completedFiles++
        }  
        
        debug(
          `File: ${completedFiles}/${filesToUpload.length}. ${
            currentFileParameters.file
          } took ${(performance.now() - startTime).toFixed(
            3
          )} milliseconds to finish upload`
        )
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

  clearInterval(totalUploadStatus)

  // done uploading, safety dispose all connections
  clientManagerInstance.disposeAllConnections()

  info(`Total size of all the files uploaded is ${fileSizes} bytes`)
  return {
    size: fileSizes,
    failedItems: failedItemsToReport
  }
}

/**
 * Asynchronously uploads a file. If the file is bigger than the max chunk size it will be uploaded via multiple calls
 * @param {number} httpClientIndex The index of the httpClient that is being used to make all of the calls
 * @param {UploadFileParameters} parameters Information about the file that needs to be uploaded
 * @returns The size of the file that was uploaded in bytes along with any failed uploads
 */
async function uploadFileAsync(
  httpClientIndex: number,
  parameters: UploadFileParameters
): Promise<UploadFileResult> {
  const originalFileSize: number = fs.statSync(parameters.file).size
  let offset = 0
  let isUploadSuccessful = true
  let failedChunkSizes = 0
  let abortFileUpload = false

  // Gzip original upload file into a temporary file that will be uplaoded
  tmp.file(async function _tempFileCreated(error, tempFilePath, fd, cleanupCallback) {
    if (error) throw error;
    
    console.log(`Temp Gzip is ${tempFilePath}`)

    const gzip = zlib.createGzip();
    const inputStream = fs.createReadStream(parameters.file);
    const gZipTempFile = fs.createWriteStream(tempFilePath)

    async function openReadStream(): Promise<void> {
      return new Promise<void>(resolve => {
        inputStream.on('open', resolve);
      })
    }

    async function pipeStreams(): Promise<number> {
      return new Promise<number> (() => {
        const gzipPipeResult = inputStream.pipe(gzip)
        gzipPipeResult.on('finish', () => {
          console.log('gzip pipe has finished')
          const tempFilePipe = gzip.pipe(gZipTempFile)
          tempFilePipe.on('finish', () => {
            return fs.statSync(tempFilePath).size
          })     
        })
      })
    }

    const gZipFileSize = await Promise.resolve(openReadStream()).then(() => {
      console.log('read stream has been opened')
      return Promise.resolve(pipeStreams()).then(() => {
        console.log('write stream has finished')
        return pipeStreams()
      })
    })

    console.log(`Gzip file size is ${gZipFileSize}`)

    let uploadFileSize = gZipFileSize
    let uploadFilePath = tempFilePath
    let isGzip = true

    if (uploadFileSize > originalFileSize){
      // compression did not help out
      uploadFileSize = originalFileSize
      uploadFilePath = parameters.file
      isGzip = false
      // delete temp Gzip file
      cleanupCallback();
    }

    console.log(`UploadFileSize is ${uploadFileSize}`)
    console.log(`UploadFilePath is ${uploadFilePath}`)

    // upload only a single chunk at a time
    while (offset < uploadFileSize) {
      const chunkSize = Math.min(gZipFileSize - offset, parameters.maxChunkSize)
      if (abortFileUpload) {
        // if we don't want to continue on error, any pending upload chunk will be marked as failed
        failedChunkSizes += chunkSize
        continue
      }

      const start = offset
      const end = offset + chunkSize - 1
      offset += parameters.maxChunkSize

      const chunk = fs.createReadStream(uploadFilePath, {
        start,
        end,
        autoClose: false
      })

      const result = await uploadChunk(
        httpClientIndex,
        parameters.resourceUrl,
        chunk,
        start,
        end,
        uploadFileSize,
        isGzip
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

    // Clean up temporary file
    if(isGzip){
      cleanupCallback();
    }
  });

  return {
    isSuccess: isUploadSuccessful,
    successfulUploadSize: originalFileSize - failedChunkSizes // TODO fix reported size
  }
}

/**
 * Uploads a chunk of an individual file to the specified resourceUrl. If the upload fails and the status code
 * indicates a retryable status, we try to upload the chunk as well
 * @param {number} httpClientIndex The index of the httpClient being used to make all the necessary calls
 * @param {string} resourceUrl Url of the resource that the chunk will be uploaded to
 * @param {NodeJS.ReadableStream} data Stream of the file that will be uploaded
 * @param {number} start Starting byte index of file that the chunk belongs to
 * @param {number} end Ending byte index of file that the chunk belongs to
 * @param {number} totalSize Total size of the file in bytes that is being uploaded
 * @param {boolean} isGzip Denotes if we are uploading a Gzip compressed stream
 * @returns if the chunk was successfully uploaded
 */
async function uploadChunk(
  httpClientIndex: number,
  resourceUrl: string,
  data: NodeJS.ReadableStream,
  start: number,
  end: number,
  totalSize: number,
  isGzip: boolean
): Promise<boolean> {
  const requestOptions = getRequestOptions(
    'application/octet-stream',
    true,
    isGzip,
    end - start + 1,
    getContentRange(start, end, totalSize)
  )

  const httpManager = HttpUploadManager.getInstance()

  const uploadChunkRequest = async (): Promise<IHttpClientResponse> => {
    return await httpManager
      .getClient(httpClientIndex)
      .sendStream('PUT', resourceUrl, data, requestOptions)
  }

  let retryCount = 0
  const retryLimit = getUploadRetryCount()

  // Allow for failed chunks to be retried multiple times
  // change this to a nice for with retryCount incrementing
  while (retryCount <= retryLimit) {
    try {
      const response = await uploadChunkRequest()
      // read the body so that the response is ac
      const testing = await response.readBody()
      console.log(testing)

      if (isSuccessStatusCode(response.message.statusCode)) {
        return true
      } else if (isRetryableStatusCode(response.message.statusCode)) {
        // dispose the existing connection and create a new one
        httpManager.disposeClient(httpClientIndex)
        retryCount++
        if (retryCount > retryLimit) {
          info(
            `Retry limit has been reached for chunk at offset ${start} to ${resourceUrl}`
          )
          return false
        } else {
          info(
            `HTTP ${response.message.statusCode} during chunk upload, will retry at offset ${start} after 10 seconds. Retry count #${retryCount}. URL ${resourceUrl}`
          )
          await new Promise(resolve => setTimeout(resolve, getRetryWaitTime()))
          httpManager.replaceClient(httpClientIndex)
        }
      } else {
        info(`#ERROR# Unable to upload chunk to ${resourceUrl}`)
        // eslint-disable-next-line no-console
        console.log(response)
        return false
      }
    } catch (error) {
      // If an error is thrown, it is most likely due to a timeout, dispose of the connection, wait and retry with a new connection
      httpManager.disposeClient(httpClientIndex)

      retryCount++
      if (retryCount > retryLimit) {
        info(
          `Retry limit has been reached for chunk at offset ${start} to ${resourceUrl}`
        )
        return false
      } else {
        info(`Retrying chunk upload after encountering an error`)
        await new Promise(resolve => setTimeout(resolve, getRetryWaitTime()))
        httpManager.replaceClient(httpClientIndex)
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
  // No concurrent calls so a single client without `keep-alive` is sufficient
  const client = createHttpClient()
  const requestOptions = getRequestOptions('application/json', false, false)
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
}

interface UploadFileParameters {
  file: string
  resourceUrl: string
  maxChunkSize: number
  continueOnError: boolean
}

interface UploadFileResult {
  isSuccess: boolean
  successfulUploadSize: number
}

/**
 * Used for managing http connections with concurrent uploads to limit the number of tcp connections created
 */
class HttpUploadManager {
  private static _instance: HttpUploadManager = new HttpUploadManager()
  private clients: HttpClient[]

  constructor() {
    if (HttpUploadManager._instance) {
      throw new Error('Error: Singleton HttpManager already instantiated')
    }
    HttpUploadManager._instance = this
    this.clients = []
  }

  static getInstance(): HttpUploadManager {
    return HttpUploadManager._instance
  }

  createClients(concurrency: number): void {
    this.clients = new Array(concurrency).fill(createHttpClient())
  }

  getClient(index: number): HttpClient {
    return this.clients[index]
  }

  disposeClient(index: number): void {
    this.clients[index].dispose()
  }

  replaceClient(index: number): void {
    this.clients[index] = createHttpClient()
  }

  disposeAllConnections(): void {
    for (const client of this.clients) {
      client.dispose()
    }
  }
}
