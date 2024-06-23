class FTPData:
    HOST = ''
    USER = ''
    PASSWD = ''

class FTPSource(FTPData):
    """
    FTPSource class represents the connection details for the source FTP server.

    Attributes:
    - HOST: The hostname of the FTP server.
    - USER: The username for authentication.
    - PASSWD: The password for authentication.
    """
    HOST = 'ftp-server'
    USER = 'source'
    PASSWD = 'source'


class FTPTarget(FTPData):
    """
        FTPTarget class represents the connection details for the target FTP server.

        Attributes:
        - HOST: The hostname of the FTP server.
        - USER: The username for authentication.
        - PASSWD: The password for authentication.
    """
    HOST = 'ftp-server'
    USER = 'target'
    PASSWD = 'target'
