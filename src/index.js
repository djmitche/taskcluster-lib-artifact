'use strict';
const assert = require('assert');
const urllib = require('url');
const {Client} = require('remotely-signed-s3');

class Artifact {
  constructor({queue, client, clientOpts}) {
    assert(typeof queue === 'object', 'Must provide a Queue client');
    this.__queue = queue;

    if (client && clientOpts) {
      throw new Error('You can only provide a client or options for a client to be created with');
    } else if (client) {
      this.__client = client
    } else if (clientOpts) {
      this.__client = new Client(clientOpts);
    } else {
      this.__client = new Client();
    }
  }

  /**
   * Upload an artifact to the queue, optionally with compression
   */
  async put({taskId, runId, name, contentType, filename, compression, expires, forceMP, forceSP}) {
    assert(typeof taskId === 'string', 'Must provide a taskId for upload');
    assert(typeof runId === 'string', 'Must provide a runId for upload');
    assert(typeof name === 'string', 'Must provide a name for upload');
    assert(typeof contentType === 'string', 'Must provide a contentType for upload');
    assert(typeof filename === 'string', 'Must provide a filename for upload');
    if (compression) {
      assert(typeof compression === 'string', 'Compression format must be string');
      assert(compression === 'gzip', 'Only gzip compression is supported');
    }
    assert(typeof expires === 'object', 'Expires must be provided as a date object');
    assert(expires.constructor.name === 'Date', 'Expires must be provided as a date object');

    let upload = await this.__client.prepareUpload({filename, compression, forceMP, forceSP});

    let requestParams = {
      storageType: 'blob',
      expires,
      contentType,
      contentSha256: upload.sha256,
      contentLength: upload.size,
    };

    if (upload.contentEncoding && upload.contentEncoding !== 'identity') {
      requestParams.transferSha256 = upload.transferSha256;
      requestParams.transferLength = upload.transferSize;
      requestParams.contentEncoding = upload.contentEncoding;
    }

    let requests = await this.__queue.createArtifact(taskId, runId, name, requestParams);
    requests = requests.requests;

    // For the time being, we do not expose the abortMultipartUpload method on
    // the Queue, so we rely on the S3 lifecycle based cleanup, which we
    // configure to delete in-process multipart uploads which are older than a
    // specified number of days.

    let outcome = await this.__client.runUpload(requests, upload);
    await this.__queue.completeArtifact(taskId, runId, name, {
      etags: outcome.etags
    });
  }

  /**
   * Get a URL from the Taskcluster Queue based on task metadata
   *
   * If runId is not given, the latest run of the taskId is used.
   */
  async get({taskId, runId, name, filename}) {
    let url;
    
    if (runId === undefined) {
      url = this.__queue.buildUrl(this.__queue.getLatestArtifact, taskId, name);
    } else {
      url = this.__queue.buildUrl(this.__queue.getArtifact, taskId, runId, name);
    }

    let {body, headers, statusCode, statusMessage} = await this.__client.runner.run({
      req: {
        url,
        method: 'GET',
        headers: {},
      },
      body: '',
    });

    // We only want to follow specific redirects
    if (![301, 302, 303, 307, 308].includes(statusCode)) {
      throw new Error('Redirect was not a valid status code (' + statusCode + ')');
    }

    let artifactUrl = headers.location;

    if (typeof artifactUrl !== 'string') {
      throw new Error('Redirect has missing Location key');
    }

    // We only want to parse the URL to ensure that it's a valid URL.  We're
    // going to avoid parsing and reserializing because that could lead to
    // subtle errors which we really don't have any reason to want to introduce
    // here
    if (urllib.parse(artifactUrl).protocol !== 'https:') {
      throw new Error('Redirect must be to an https resource, but got ' + artifactUrl);
    }

    await this.__client.downloadUrl({url: artifactUrl, output: filename});
  }

  /**
   * This function is for those cases where we're getting a URL from
   * somewhere other than the queue.  This could be an indexed queue url.
   * In short, we want to have all the download verification without
   * needing to build the URL
   */
  async getUrl({url, filename}) {
    await this.__client.downloadUrl({url, output: filename});
  }
}


module.exports = {Artifact};
