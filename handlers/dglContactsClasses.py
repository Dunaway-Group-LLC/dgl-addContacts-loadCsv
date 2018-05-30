#
# # Definition of Contacts objects
#

#
# # imports
#
import boto3
from botocore.exceptions import ClientError, ParamValidationError
from handlers.dglPickleToS3BucketClasses import S3pickleBucket
import pickle
from io import BytesIO
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#
# # class defs
#


class Contact:
    """class Contact
            first_name, last_name, email, attrs
                atttrs depends on application using the object
    """

    def __init__(
            self, email,  first_name="",  last_name="",  product="", attrs={}):
        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        self.product = product    # Name of product associated with Contact
        self.attrs = attrs    # new empty dict of attributes for each contact


class Contacts:

    """class Contacts - holds all contacts for all Products
        - stored pickled in s3 bucket dgl-contacts
    """
#
# # function defs
#

    def __init__(self,  bucketName, keyName):
        """__init__(bucketName)
            create instance of Contacts with bucketName/keyName
            contacts instance var is init to empty dict
        """
        self.contacts = {}          # Dictionary holding Contacts, key is email
        self.bucketName = bucketName    # Bucket name holding Contacts object
        self.keyName = keyName      # keyName / folder / object

    def addContact(self,  contact):
        """ addContact(contact)
            contact must contail email; can be otherwise empty
            key - contact.email - must not be in Contacts
        """
        key = contact.email         # email is key for Contacts dict
        if key in self.contacts:    # Prevent dupes
            return(False)
        else:
            self.contacts[key] = contact
            return(True)

    def getContact(self, contact):
        """getContact(contact)
            parm contact has only email
                is returned filled
        """
        key = contact.email
        if key in self.contacts:
            contact = self.contacts[key]
            return(True)
        else:
            return(False)

    def updateContact(self,  contact):
        """updateContact(contact)
            parm contact must have email that is key in Contacts
            returns True if key found and Contact updated
            returns False if key not in Contacts
        """
        key = contact.email
        if key in self.contacts:
            self.contacts[key] = contact
            return(True)
        else:
            return(False)

    def loadContacts(self, s3pickle_bucket):
        """loadContacts(s3PickleBucket)
                Gets pickled Contacts object from S2
                unpickle
                returns Contacts

                 Boto 3

        """
#        s3 = boto3.resource('s3')   # get S3.Object
#
# # Prove we can talk to bucket
#
#       bucket = s3.Bucket(self.bucketName)
#        for obj in bucket.objects.all():
#            print("bucket keys", obj.key)

#        self.contacts = BytesIO()   # unpickled comes as bytes
        self.contacts = s3pickle_bucket.loadObject("contacts")
        if isinstance(self.contacts, dict):
            return(self)
        else:
            self.contacts = self.createContactsObject(self.bucketName)
            # create new obj
            return(self)

#
# # Pickle and store Contacts

    def storeContacts(self):
            """
                Pickle and save in s3
            """
            s3 = boto3.resource('s3')                   # get S3.Object
            print("Bucket Name:", self.bucketName)
            body = pickle.dumps(self.contacts)     # serialized Contacts dict
    # Store contacts with firm email seperate, lookup pers email later
            if self.bucketName == "firm-contacts":
                objid = self.bucketName
                self.bucketName = "dgl-contacts"
            else:
                objid = "contacts"
            try:
                s3.Object(self.bucketName, objid).put(Body=body)
            except ParamValidationError as e:
                print("Parameter validation error: %s" % e)
            except ClientError as e:
                print("Unexpected error: %s" % e)
                print(e.response['Error']['Code'])



    def confirmContact():
            """confirmContact()
                    Sends SES email to new contact
            """
            print("In confirmContact")
            pass

    def createContactsObject(self, bucketName):
        """
        Create Contacts - put new Contacts object in S3 bucket 'dgl-contacts'
        """
        logger.info(">>> bucket type: {}".format(type(bucketName)))

        s3 = boto3.resource('s3')                   # get S3.Object

        contacts = Contacts(bucketName, "contacts")  # Contacts obj empty dict
        body = pickle.dumps(contacts)      # serialized Contacts object
        try:
            s3.Object(contacts.bucketName, 'contacts').put(Body=body)
            return(contacts)
        except ParamValidationError as e:
            print("Parameter validation error: %s" % e)
        except ClientError as e:
            print("Unexpected error: %s" % e)
            print(e.response['Error']['Code'])




"""

pickle_buffer = BytesIO()
s3_resource = boto3.resource('s3')

new_df.to_csv(pickle_buffer, index=False)
s3_resource.Object(bucket,path).put(Body=pickle_buffer.getvalue())

"""


class Product:
    """class Product - a Product is something being marketed
            name - string
            owner - Person responsible for Product
            desc - description of the Product
            campaigns - dictionary of Campaigns
            dates - dictionary of dates {release:date, ???}
    """

    def __init__(self, name, owner,  desc, release_date):
        self.name = name
        self.desc = desc
        self.dates = {"start_date": start_date, "due_date": due_date}

    def set_dates(self, dates):
        self.dates = dates


class Message:
    """
        class Message
        name, desc, text, freebie
    """

    def __init__(self, name, desc, text,  freebie):
        self.name = name
        self.desc = desc
        self.text = text
        self.freebie = freebie              # Something the user can download


class Campaign:
    """class Campaign - a Campaign - holds some Messages, last_sent[date_time,
        message_name], interval for sending
           name, desc, last_sent, interval

    """

    def __init__(self, name,  desc,  interval):
        self.name = name
        self.desc = desc
        self.interval = interval
        self.messages = {}
        self.last_sent = [0, "none"]
        self.messages = []


class Campaigns:
    """class Campaigns - contains all campaigns - stored pickled in s3
            campaigns

    """

    def __init__(self):
        self.campaigns = {}         # All campaigns - name : Campaign

    def loadCampaigns(self, campaigns):
        """loadContacts(contacts)  # contacts is empty instance of Campaigns
                Checks that self.campaigns = {}
                Gets pickled Campaigns object from S3
                unpickle
                returns self.campaigns

                 Boto 3

        """
        pass

    def storeCampaigns(self):
        """
            Pickle and save in s3
        """

    def addCampaign(self, name, desc,  interval):
        if name in self.campaigns:
            return False
        else:
            self.campaigns[name] = Campaign(name, desc, interval)
            return True

    def delCampaign(self, name):
        if name in self.campaigns:
            del self.campaigns[name]
            return True
        else:
            return False

    def chgCampaign(self, name, desc, interval, messages):
        if name in self.campaigns:
            self.campaigns[name] = Campaign(name, desc, interval)
            self.campaigns[name].messages = messages
            return True
        else:
            return False
