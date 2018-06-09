"""Wrapper for pickle/unpickel to S3 Bucket via boto3
    - to localize S3 exceptions and connections
    - to solve BytesIO issues once
"""
#
# # imports
#

import boto3
import botocore
import pickle
from io import BytesIO, StringIO
import logging
from sys import getsizeof

logger = logging.getLogger()
logger.setLevel(logging.INFO)

"""Wraps S3 bucket operations, exceptions and connections
    - a Singleton object - so there's just one connection
    - wraps only get/put Body
    - includes pickle/unpickel of BytesIO streams
"""


class S3pickleBucket():
    """S3 Bucket wrapper class


    """

    def __init__(self, bucketName, S3):
        self.bucketName = bucketName
        self.pickled = BytesIO
        self.s3 = S3
#
# # Pickle and store Contacts

    def storeObject(self, obj, objid):
            """Pickle and save in s3 bucket wrapped by this instance
                obj is class instance to be pickled & stored
                objid is s3 key

            """
            logger.info("In storeObject - {} / {}".format(obj, type(obj)))

            self.pickled = pickle.dumps(obj)     # serialize object

            print("self.pickled {} {} {}".format(
                self.pickled, type(self.pickled), getsizeof(self.pickled)))
    # Store pickled object
            try:
                self.s3.Object(self.bucketName, objid).put(Body=self.pickled)
                return(0)
            except botocore.exceptions.ClientError as e:
                # If a client error thrown, then check it was a 404 error.
                # If it was a 404 error, then the bucket does not exist.
                error_code = int(e.response['Error']['Code'])
                if error_code == 404:
                    logging.error("Specified bucket does not exist")
                    return("Error code: ", error_code)  # go to the house

    def loadObject(self, objid):
            """Load & unpickle from s3 bucket wrapped by this instance
                objid is s3 key
                returns unpickled object
                """
            logger.info("loadObject>>>objid {} type {}".format(
                objid, type(objid)))
            unpickled = ""
            try:
                for obj_id in self.s3.Bucket(self.bucketName).objects.all():
                    print("obj_id: ", obj_id)   # print object in bucket
                    # logger.info("bucket key: {}").format(obj)

                print("self.pickled", self.pickled, type(self.pickled))
                self.pickled = self.s3.Object(self.bucketName, objid)  # FO
                logger.info("aft get self.pickled {} / {} / {} / {}".format(
                    self.pickled, type(self.pickled),
                    self.pickled.content_length, self.pickled.content_type))
                body = self.pickled.get()['Body'].read()   # content
                print("Body", body, type(body))
                unpickled = pickle.loads(body)
                print("unpickled", unpickled, type(unpickled))

                return(unpickled)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    logging.error("Object %s does not exist" % (objid))
                else:
                    quit(e.response['Error']['Code'])


def getPickleBucket(bucketName):
    """Get S3pickleBucket - module function pseudo-singleton
        - connect
        - or create bucket and connect, if none

    """
    bn = bucketName    # Bucket name holding / to hold object
    s3 = boto3.resource('s3')   # get S3.Object
    logging.debug("s3 %s" % type(s3))
    try:
        logging.debug("bucketName %s" % (bn))
        pb = S3pickleBucket(bn, s3)  # create instance with bucketName, S3 ref
        s3.meta.client.head_bucket(Bucket=bn)
        return(pb)    # return the bucket object
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            logging.error("Specified bucket does not exist")
            return(error_code)  # go to the house
