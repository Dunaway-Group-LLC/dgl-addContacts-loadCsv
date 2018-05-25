#
# # AWS Lambda Handler 
#

#
# # Customization
#
BUCKET = "dgl-contacts"             # the bucket where DGL Contacts are stored
DOMAIN_NAME = "foolsorknaves.com" # The domain for this instance
print("dglContacts loaded", BUCKET, DOMAIN_NAME) 

from dglContactsClasses import  Contact,  Contacts,  loadContacts

def addContact(event, context): 
    
    first_name = event['queryStringParameters']["first_name"];
    last_name = event['queryStringParameters']["last_name"]
    email = event['queryStringParameters']["email"]
    reviewer = event['queryStringParameters']["reviewer"] == ("Y" or "y") # True if == Y or y
    product = event['queryStringParameters']["product"]
    
    print(first_name," / ",last_name," / ",email," / ",reviewer," / ",product)
    
    contact = Contact(first_name,  last_name,  email,  reviewer,  product) # The new Contact
    contacts = Contacts(BUCKET)     # S3 bucket which contains all DGL Contacts
    loadContacts(contacts)
    addContact(contact)
    return {}
