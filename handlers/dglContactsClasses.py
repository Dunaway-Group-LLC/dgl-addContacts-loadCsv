#
# # Definition of Contacts object
# #     Later: getters & putters
#

#
# # imports
#
import boto3
from botocore.exceptions import ClientError
import pickle
from io import BytesIO
   
#
# # class defs
#
class Contact:
    """class Contact
            first_name, last_name, email, attrs
                atttrs depends on application using the object
    """
    def __init__(self, email,  first_name="",  last_name="",  product=""):
        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        self.product = product              # Name of product associated with Contact
        self.attrs = []    #new empty list of attributes for each contact
        
        
class Contacts:
    def __init__(self,  bucketName):
        self.contacts = {}        # Dictionary holding Contacts, key is email
        self.bucketName = bucketName    # Bucket name holding Contacts object

        
    def addContact(self,  Contact):
        print("Add Contact")
        pass
        
    def getContact(self,  Contact):      
        pass
    
    def updateContact(self,  Contact):
        pass

#
# # function defs
#
    def loadContacts(contacts):
        """loadContacts(contacts)  # contacts is empty instance of Contacts
                Gets pickled Contacts object from S2
                unpickle
                returns Contacts
                
                 Boto 3
                
        """
        s3 = boto3.resource('s3') # get S3.Object
#
# # Prove we can talk to bucket
#
        bucket = s3.Bucket(contacts.bucketName)
        for obj in bucket.objects.all():
            print(obj.key)
            
        contacts = BytesIO()   # unpickled comes as bytes   
        try:
             contacts = s3.Object(contacts.bucketName, 'contacts').get()["Body"].read() # get pickled Contacts
             contacts = pickle.load(contacts)  # unpickle
        except ClientError:
           print("The object does not exist.")
           createContactsBucket(contacts.bucketName) #No existing dgl-contacts bucket
        return(contacts)  
    
def confirmContact():
        """confirmContact()
                Sends SES email to new contact
        """
        print("In confirmContact")
        pass

def createContactsBucket(bucket):
    
    """
        Create Contacts - put new Contacts object in S3 bucket 'dgl-contacts'
    """
    print("bucket type:",type(bucket))
    
    s3 = boto3.resource('s3')                   # get S3.Object
    
    contacts = Contacts(bucket)               # Contacts object with empty dictionary
    body = pickle.dumps(contacts)           # serialized Contacts object
    try:
        s3.Object(contacts.bucket, 'contacts').put(Body=body)
    except Exception as e:
        print(type(e))
        print(e.args)
        print(e)
        
    
    for obj in contacts.bucket.objects.all():
        print(obj.key) 

"""
pickle_buffer = BytesIO()
s3_resource = boto3.resource('s3')

new_df.to_csv(pickle_buffer, index=False)
s3_resource.Object(bucket,path).put(Body=pickle_buffer.getvalue())

"""
