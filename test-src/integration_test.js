const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const {Artifact} = require('../');
const {Queue} = require('./mock-queue');

async function writeFileOfSize(size, name) {
  let bytes = await new Promise((resolve, reject) => {
    crypto.randomBytes(size, (err, buf) => {
      if (err) {
        return reject(err);
      }
      return resolve(buf);
    });
  });
  fs.writeFileSync(name, bytes);
}

function checkFilesEqual(a, b) {
  a = fs.readFileSync(a);
  b = fs.readFileSync(b);
  if (a.length !== b.length) {
    throw new Error('Files are different sizes');
  }
  for (let i = 0 ; i <= a.length ; i++) {
    if (a[i] !== b[i]) {
      throw new Error(`Files differ at byte ${i}: ${a[i]} !== ${b[i]}`);
    }
  }
}

describe('taskcluster-lib-artifact', () => {
  let queue;
  let subject;
  let uploadFilename = 'up.dat';
  let downloadFilename = 'down.dat';
  let expires = new Date();
  expires.setDate(expires.getDate() + 1);

  beforeEach(async () => {
    queue = new Queue({bucket: 'test-bucket-for-any-garbage', port: 30000});
    await queue.start();
    subject = new Artifact({queue});
  });

  afterEach(async () => {
    await queue.stop();
  });

  describe('helper functions', () => {
    it('should write out a file of a size', async () => {
      await writeFileOfSize(1024, uploadFilename);
      if (fs.statSync(uploadFilename).size !== 1024) {
        throw new Error('Wrote the wrong number of bytes');
      }
    });

    it('should check files equal', () => {
      checkFilesEqual(__filename, __filename);
      try {
        checkFilesEqual(__filename, path.join(__dirname, '..', 'package.json'));
        throw new Error();
      } catch (err) {
        if (!/^Files /.test(err.message)) {
          throw err;
        }
      }
    });
  });

  describe('without compression', () => {
    it('should upload and download a file without compression in a single part', async () => {
      await writeFileOfSize(10 * 1024 * 1024, uploadFilename);
      await subject.put({
        taskId: 'task',
        runId: 'run',
        name: 'name',
        contentType: 'application/octet-stream',
        filename: uploadFilename,
        expires,
        forceSP: true,
      });
      await subject.get({
        taskId: 'task',
        runId: 'run',
        name: 'name',
        filename: downloadFilename,
      });
      checkFilesEqual(uploadFilename, downloadFilename);
    });

    it('should upload and download a file without compression with multiple parts', async () => {
      await writeFileOfSize(10 * 1024 * 1024 + 1, uploadFilename);
      await subject.put({
        taskId: 'task',
        runId: 'run',
        name: 'name',
        contentType: 'application/octet-stream',
        filename: uploadFilename,
        expires,
        forceMP: true,
      });
      await subject.get({
        taskId: 'task',
        runId: 'run',
        name: 'name',
        filename: downloadFilename,
      });
      checkFilesEqual(uploadFilename, downloadFilename);
    });
  });

  describe('with compression', () => {
    it('should upload and download a file with compression in a single part', async () => {
      await writeFileOfSize(10 * 1024 * 1024, uploadFilename);
      await subject.put({
        taskId: 'task',
        runId: 'run',
        name: 'name',
        contentType: 'application/octet-stream',
        filename: uploadFilename,
        expires,
        compression: 'gzip',
        forceSP: true,
      });
      await subject.get({
        taskId: 'task',
        runId: 'run',
        name: 'name',
        filename: downloadFilename,
      });
      checkFilesEqual(uploadFilename, downloadFilename);
    });

    it('should upload and download a file with compression in multiple parts', async () => {
      await writeFileOfSize(10 * 1024 * 1024 + 1, uploadFilename);
      await subject.put({
        taskId: 'task',
        runId: 'run',
        name: 'name',
        contentType: 'application/octet-stream',
        filename: uploadFilename,
        expires,
        compression: 'gzip',
        forceMP: true,
      });
      await subject.get({
        taskId: 'task',
        runId: 'run',
        name: 'name',
        filename: downloadFilename,
      });
      checkFilesEqual(uploadFilename, downloadFilename);
    });
  });
});
