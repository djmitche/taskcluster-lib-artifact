const assume = require('assume');
const {Controller} = require('remotely-signed-s3');

const BUCKET = 'test-bucket-for-any-garbage';

/**
 * This is an object which pretends to be the queue.  NOTE: This queue implementation
 * only supports content type 'application/octet-stream'
 */
class Queue {
  // exp = expected, C = content, T = transfer, L = length, Sha = Sha256
  constructor() {
    this.controller = new Controller({region: 'us-west-2'});
  }

  async createArtifact(taskId, runId, name, body) {
    if (this.inprogress) {
      throw new Error('This mock only allows one in progress upload');
    }
    this.inprogress = true;
    assume(taskId).to.be.a('string');
    assume(runId).to.be.a('string');
    assume(runId).to.be.a('string');
    assume(body).to.be.an('object');
    let key = [taskId, runId, name].join('/');
    if (body.parts) {
      let uploadId = await this.controller.initiateMultipartUpload({
        bucket: BUCKET,
        key,
        sha256: body.contentSha256,
        size: body.contentLength,
        transferSha256: body.transferSha256,
        transferSize: body.transferLength,
        contentType: body.contentType,
        contentEncoding: body.contentEncoding
      });
      let requests = await this.controller.generateMultipartRequest({
        bucket: BUCKET,
        key,
        uploadId,
        parts: body.parts,
      });
      return {
        storageType: 'blob',
        expires: 'lala',
        requests
      };
    } else {
      let request = await this.controller.generateSinglepartRequest({
        bucket: BUCKET,
        key,
        sha256: body.contentSha256,
        size: body.contentLength,
        transferSha256: body.transferSha256,
        transferSize: body.transferLength,
        contentType: body.contentType,
        contentEncoding: body.contentEncoding
      });
      return {
        storageType: 'blob',
        expires: 'lala',
        requests: [request]
      };
    }
  }

  async completeArtifact(taskId, runId, name, body) {
    if (!this.inprogress) {
      throw new Error('Must have upload in progress to complete it');
    }
    delete this.inprogress;
    assume(taskId).to.be.a('string');
    assume(runId).to.be.a('string');
    assume(runId).to.be.a('string');
    assume(body).to.be.an('object');
    let key = [taskId, runId, name].join('/');
    if (this.uploadId) {
      await this.controller.completeMultipartUpload({
        bucket: BUCKET,
        key,
        etags: body.etags,
        uploadId: this.uploadId,
      });
    }
  }

  buildUrl(method, taskId, runId, name) {
    return `https://${BUCKET}.s3-us-west-2.amazonaws.com/${taskId}/${runId}/${name}`;
  } 
}


module.exports = {Queue};
