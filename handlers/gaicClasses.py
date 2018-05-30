#
# # GAIC dglContactsClasses
#
#
# # class FirmEmails - list of emails identified as going to firms rather
# # than individuals
# # class loads list from file at __init__
#
from handlers.dglPickleToS3BucketClasses import S3pickleBucket


class FirmEmails():
    """FirmEmails holds a list of email domains known to belong to firms,
    rather than individuals
    """

    def __init__(self, pb):
        """pb - S3pickleBucket give ref to where domains list is stored
        """
        self.pb = pb
        self.firm_domains = pb.loadObject("firm-domains")

    def inFirmEmails(self, email_domain):
        """Return true if email_domain is in firm_emails
        """
        if email_domain in self.firm_domains:
            return True
        else:
            return False
