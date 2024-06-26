from ftplib import FTP
import os
from datetime import datetime
from configs.ftps import FTPData

LOCAL_PATH = '/tmp'


def _get_status_file_folder(ftp_s, folder_s, current_locate):
    """
        Get lastest status of files and folders on a source FTP

        :param folder_s: Đường dẫn FTP Source
        :param current_locate: File cần xét
        :return current_state: Ngày Lastest Update của File/ Folder
    """
    s_file_list = []
    ftp_s.retrlines(f'LIST {folder_s}', s_file_list.append)
    current_state = str(datetime.now().year)
    for k in s_file_list:
        if current_locate in k:
            s_file_list = k

    for j in s_file_list.split()[5:7]:
        current_state += j
    current_state = datetime.strptime(current_state, "%Y%b%d")
    current_state = current_state.strftime("%Y%m%d")
    return current_state


def _check_folder_ftp(ftp_s, ftp_t, folder_s, folder_t, exec_date_date_key):
    ftp_s.cwd(folder_s)
    """
        Recursively integrate files and folders from a source FTP server to a target FTP server.

        :param folder_s: Đường dẫn FTP Source
        :param folder_t: Đường dẫn FTP Target
        :param exec_date_date_key: Ngày chạy trên AirFlow
        """
    try:
        ftp_t.cwd(folder_t)
    except:
        ftp_t.mkd(folder_t)
        ftp_t.cwd(folder_t)

    #    Check File/Folder đã bị xóa trên con FTP Source hay chưa
    for k in ftp_t.nlst():
        if 'deleted' in k:
            continue
        if k not in ftp_s.nlst():
            tmp = ''
            if '.' in k:
                tmp = k.split('.')
                tmp = tmp[0] + '_deleted_' + exec_date_date_key + '.' + tmp[1]
                ftp_t.rename(k, tmp)
            else:
                tmp = k + '_deleted' + exec_date_date_key
                ftp_t.rename(k, tmp)

    # Excute
    for i in ftp_s.nlst():
        # Check File hay Folder
        if '.' in i:
            ftp_s.cwd(folder_s)
            ftp_t.cwd(folder_t)
            if i in ftp_t.nlst():
                # So sánh ngày chạy airflow so với ngày thay đổi của file, ngày chạy airflow = ngày update file
                current_state = _get_status_file_folder(ftp_s, folder_s, i)
                if current_state == exec_date_date_key:
                    ftp_s.retrbinary("RETR " + folder_s + '/' + i, open(i, "wb").write)
                    ftp_t.storbinary("STOR " + folder_t + '/' + i, fp=open(i, "rb"))
            else:
                ftp_s.retrbinary("RETR " + folder_s + '/' + i, open(i, "wb").write)
                ftp_t.storbinary("STOR " + folder_t + '/' + i, fp=open(i, "rb"))
        else:
            # So sánh ngày chạy airflow so với ngày thay đổi của folder, ngày chạy airflow = ngày update folder
            current_state = _get_status_file_folder(ftp_s, folder_s, i)
            if current_state == exec_date_date_key:
                _check_folder_ftp(ftp_s, ftp_t, folder_s + '/' + i, folder_t + '/' + i, exec_date_date_key)


def intergrate_ftp_server(source: FTPData, target: FTPData, **kwargs):
    """
        Integrate files and folders from a source FTP server to a target FTP server.

        :param source: FTP connection details for the source server.
        :param target: FTP connection details for the target server.
    """
    excution_time = kwargs["execution_date"]
    exec_date_date_key = excution_time.strftime("%Y%m%d")
    os.chdir(LOCAL_PATH)
    ftp_s = FTP(host=source.HOST, user=source.USER, passwd=source.PASSWD)
    ftp_t = FTP(host=target.HOST, user=target.USER, passwd=target.PASSWD)
    folder_s = ftp_s.pwd()
    folder_t = ftp_t.pwd()
    _check_folder_ftp(ftp_s, ftp_t, folder_s, folder_t, exec_date_date_key)
