"""dgl-addContacts-loadCsv
triggered by S3 - object loaded - a csv file of new Contacts
"""

import logging
import boto3
from handlers.dglContactsClasses import Contact, Contacts
from handlers.dglPickleToS3BucketClasses import S3pickleBucket
import io
from handlers.gaicClasses import FirmEmails
import csv

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.resource('s3')
logger.info("Hi there! before lambda_handler")


def lambda_handler(event, context):

    logger.info("Hi there!")
    # retrieve bucket name and file_key from the S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    logger.info('Reading {} from {}'.format(file_key, bucket_name))

    # get the .csv file object
# get the .csv file object
    # csvFileObject = s3.get_object(Bucket=bucket_name, Key=file_key)
    filename = 'csvFile'
    s3.Object(bucket_name, file_key).download_file(filename)

    csvFile = open('csvFile')

    # bucket = s3.Bucket(bucket_name)
    # logger.info("s3Object type {} data {}".format(
    #     type(s3Object), s3Object))
    #
    # s3ObjectBody = s3Object.get()['Body'].read()
    # logger.info("csvFileObject type {} data {}".format(
    #     type(s3ObjectBody), s3ObjectBody))
    #
    # # Convert bytes object to BytesIO
    # csvFileObject = s3ObjectBody.decode()
    # print(">>>>csvFileObject", type(csvFileObject))

    contactsPers = Contacts(bucket_name, file_key)  # contacts-pers email

    contactsFirm = Contacts(bucket_name, "firm-contacts")
    # Will be stored in dgl-contacts bucket with object id firm-contacts

    # Create pickle bucket
    pb = S3pickleBucket(bucket_name, s3)
    # logger.info("S3pickleBucket", type(pb))
    # load the existing Contacts
    contactsPers = contactsPers.loadContacts(pb)
    if contactsPers.contacts == {}:     # couldn't load Contacts
        raise FileNotFoundError("Unable to load Contacts object")
    firm_emails = FirmEmails(pb)  # list of email domains from ins firms
    if firm_emails == []:
        raise FileNotFoundError("Unable to load FirmEmails object")

    # read and map the csv to Contact objects
    readCsv(csvFile)

    # Store the new contactsFirm - both pers & firm_emails
    contactsPers.storeContacts()
    contactsFirm.storeContacts()


def readCsv(csvFileObject):
    """
    .csv header for download from GAIC
    License Number,License Type,First Name,Middle Name,Last Name,
    Line One Address,Line Two Address,City,State,Zip,NPN,Business Tel,
    Email,Qualification Date,Expiration Date

    """

    reader = csv.DictReader(csvFileObject)

    for row in reader:
        print("row: ", type(row), row)
        print(row['First Name'], row['Last Name'])  # Just test reading
        # Create Contact for each row in .csv- incomplete
        contact = Contact(
            row["Email"], row["First Name"],
            row["Last Name"], "PRODUCT",
            {
                "Qualification Date": row["Qualification Date"],
                "Expiration Date": row["Expiration Date"]
            }
        )
        logger.info('Contact: {} '.format(
            contact.first_name,
            contact.last_name,
            contact.email)
            )
        print(
            "Contact: ", contact.first_name, contact.last_name,
            contact.email)
        # Add each to Contacts - either pers or firm by email domain
        if contact.email == "n/a":
            contact.email = "none@none.com"
        email_domain = contact.email.split("@")[1]
        logger.info('Domain {} '.format(email_domain))
        # print("Domain", email_domain)
        if firm_emails.inFirmEmails(email_domain):
            contactsFirm.addContact(contact)
        else:
            contactsPers.addContact(contact)
