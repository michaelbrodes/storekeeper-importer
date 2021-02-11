const https = require("https");
const {
    S3Client,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
    PutObjectCommand } = require("@aws-sdk/client-s3");

const region = process.env.REGION;
const bucketTemplate = process.env.BUCKET_TEMPLATE;
// valid values are "development", "staging", and "production".
const environment = process.env.NODE_ENV;

const s3client = new S3Client({ region: region });

/**
 * Gets the S3 bucket for this environment.
 * @returns {string}
 */
function getS3Bucket() {
    return `${bucketTemplate}-${environment}`;
}

/**
 * Downloads the full product CSV from openfoodfacts.
 *
 * @param {number} syncTime The current time in epoch seconds.
 */
async function downloadProductsToS3(syncTime) {
    const bucket = getS3Bucket();
    const objectKey = `product-import/${syncTime}-products.csv`

    const createMultipart = new CreateMultipartUploadCommand({
        Bucket: bucket,
        ContentType: "text/csv",
        Key: objectKey
    });
    const uploadId = (await s3client.send(createMultipart)).UploadId;

    const options = {
        host: "static.openfoodfacts.org",
        path: "/data/en.openfoodfacts.org.products.csv",
        method: "GET"
    };

    const promises = [];
    let partNumber = 1;
    const callback = (response) => {
        response.on('data', (chunk) => {
            const uploadPartCommand = new UploadPartCommand({
                Bucket: bucket,
                Key: objectKey,
                Body: chunk,
                UploadId: uploadId,
                PartNumber: partNumber
            })
            promises.push(s3client.send(uploadPartCommand));
            partNumber++;
        })

        response.on("end", async () => {
            await Promise.all(promises);

            const completeMultipart = new CompleteMultipartUploadCommand({
                Bucket: bucket,
                Key: objectKey,
                UploadId: uploadId
            });

            return s3client.send(completeMultipart);
        })
    }

    const clientRequest = https.request(options, callback);
    clientRequest.on("error", (e) => {
        console.error("Failed to download products from openfoodfacts", e);
    });
    clientRequest.end();
}

/**
 * Write an object to S3 that records the time that this sync was initiated.
 *
 * @param {number} syncTime The time that the sync was initiated.
 * @return {Promise} the write request.
 */
function writeLatestSyncObject(syncTime) {
    const putObject = new PutObjectCommand({
        Bucket: getS3Bucket(),
        Body: syncTime.toString(),
        ContentType: "text/plain",
        Key: "product-import/last-sync"
    });

    return s3client.send(putObject);
}

exports.lambdaHandler = async () => {
    try {
        // Current time in seconds since epoch.
        const syncTime = new Date().getTime() / 1000;
        await downloadProductsToS3(syncTime);
        await writeLatestSyncObject(syncTime);

        return {
            statusCode: 200
        };
    } catch (e) {
        console.error("Failed to write product database to S3.", e)
        return {
            statusCode: 500
        }
    }
}
