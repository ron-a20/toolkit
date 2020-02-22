import {ArtifactClient, DefaultArtifactClient} from './internal-artifact-client'
import {UploadResponse} from './internal-upload-response'
import {UploadOptions} from './internal-upload-options'
import {DownloadResponse} from './internal-download-response'
import {DownloadOptions} from './internal-download-options'

export {
  ArtifactClient,
  UploadResponse,
  UploadOptions,
  DownloadOptions,
  DownloadResponse
}

/**
 * Constructs an ArtifactClient
 */
export function create(): ArtifactClient {
  return DefaultArtifactClient.create()
}
