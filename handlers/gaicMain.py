#! /usr/bin/python3.6
#
# # Reads downloaded .csv from GAIC website, containing agents contact info
# # Extracts new licenses for just ended month (parameter)
# # Creates two Contacts objects - one to hold Contact(s) with personal email
# #  [ contactsPers ]
# # and one to hold those with company emails [ contactsFirm ]
# # Creates Contact object from each line in .csv
# # # Checks email on new Contact and adds it to contactsPers or contactsFirm
# # # based on FirmEmails
#
# /home/les/Downloads/GAIC-LifeLic-CurrentMonth.csv
# # imports
# will it save? yes it did

import sys
import getopt
import csv
from dglContactsClasses import Contact, Contacts
from gaicClasses import FirmEmails
from dglPickleToS3BucketClasses import S3pickleBucket, getPickleBucket


def readCsv(csvFileObject):
    """
    .csv header for download from GAIC
    License Number,License Type,First Name,Middle Name,Last Name,
    Line One Address,Line Two Address,City,State,Zip,NPN,Business Tel,
    Email,Qualification Date,Expiration Date

    """

    reader = csv.DictReader(csvFileObject)

    for row in reader:
        # print(row['First Name'], row['Last Name'])# Just test reading
        # Create Contact for each row in .csv- incomplete
        contact = Contact(
            row["Email"], row["First Name"],
            row["Last Name"], "PRODUCT",
            {
                "Qualification Date": row["Qualification Date"],
                "Expiration Date": row["Expiration Date"]
            }
        )
        print(
            "Contact: ", contact.first_name, contact.last_name,
            contact.email)
        # Add each to Contacts - either pers or firm by email domain
        if contact.email == "n/a":
            contact.email = "none@none.com"
        email_domain = contact.email.split("@")[1]
        print("Domain", email_domain)
        if firm_emails.inFirmEmails(email_domain):
            contactsFirm.addContact(contact)
        else:
            contactsPers.addContact(contact)
# Store the new contactsFirm - both pers & firm_emails
    contactsPers.storeContacts()
    contactsFirm.storeContacts()
