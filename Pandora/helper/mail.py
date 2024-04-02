import datetime
import datetime as dt
import poplib
import smtplib
import telnetlib
import time
from dataclasses import dataclass
from email.header import Header, decode_header
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.parser import Parser
from email.utils import parseaddr, formataddr
from pathlib import Path
from typing import List

from Pandora.helper.config import Envs, Models
from Pandora.constant import Consts, Section, TP
from Pandora.helper.date import Dates, DateFmt


@dataclass
class MailConfig:
    """邮箱配置模型"""
    host: str
    port_smtp: int
    port_pop3: int
    from_user: str
    username: str
    password: str


class MailSender:
    def __init__(self, subject: str, to: TP.TSeqStr, cc: TP.TSeqStr = None):
        assert subject, "email subject can't be empty!"
        assert to, "email receivers can't be empty!"

        cc = cc or []
        self.cfg = Models.mapping(MailConfig, Section.MAIL)
        self.msg = MIMEMultipart()
        self.msg['Subject'] = Header(subject, Consts.CHAR_UTF8).encode()
        self.msg['From'] = MailSender.format_addr(f'量化CTA提醒 <{self.cfg.from_user}>')
        self.msg['To'] = ",".join([MailSender.format_addr(em) for em in to])
        self.msg['Cc'] = ",".join([MailSender.format_addr(em) for em in cc]) if cc else ""
        self.msg['Date'] = f'{dt.datetime.now()}'
        print(f"mail to: {to}\ncc: {cc}\nsubject: {subject}\n")

    def add_text(self, text_plain, encoding=Consts.CHAR_UTF8):
        text_plain = MIMEText(text_plain, 'plain', encoding)
        self.msg.attach(text_plain)

    def add_img(self, img_path):
        assert Path(img_path).exists(), f"image {img_path} not exists!"
        image = MIMEImage(Path(img_path).read_bytes())
        image.add_header('Content-ID', '<image1>')
        image["Content-Disposition"] = 'attachment; filename="{}"'.format(Path(img_path).name)
        self.msg.attach(image)

    def add_html(self, html_code, encoding=Consts.CHAR_UTF8):
        text_html = MIMEText(html_code, _subtype='html', _charset=encoding)
        self.msg.attach(text_html)

    def add_file(self, filepath, encoding=Consts.CHAR_UTF8):
        fl = Path(filepath)
        assert fl.exists(), "The attachment file to be added does not exist!"
        text_att = MIMEText(fl.read_bytes(), 'base64', encoding)
        text_att.add_header("Content-Type", 'application/octet-stream')
        text_att.add_header('Content-Disposition', 'attachment', filename=(encoding, '', fl.name))
        self.msg.attach(text_att)

    def send(self):
        with smtplib.SMTP() as smtp:
            try:
                smtp.connect(host=self.cfg.host, port=self.cfg.port_smtp)
                # smtp.set_debuglevel(1)  # 输出详细信息
                smtp.login(self.cfg.username, self.cfg.password)
                smtp.sendmail(self.msg['From'], self.msg['To'].split(','), self.msg.as_string())
                smtp.quit()
                print(">>>>> Send mail success!")
            except smtplib.SMTPException as ex:
                print("send mail failed!", ex.args)

    @staticmethod
    def format_addr(s_addr: str) -> str:
        """对email进行格式化. s_addr 支持 'name <name@xx.bb>'和 name@xx.bb 两种格式"""
        assert s_addr and s_addr.find("@") != -1, "email address format error!"
        if s_addr.find("<") != -1 and s_addr.rfind(">") != -1:
            name, addr = parseaddr(s_addr)
        else:
            name, addr = s_addr.split("@")[0], s_addr
        return formataddr((Header(name, Consts.CHAR_UTF8).encode(), addr))

    @staticmethod
    def send_body(subject: str, body: str, to: TP.TSeqStr, *, cc: TP.TSeqStr = None, files: TP.TSeqPath = None):
        cc, files = cc or [], files or []
        sender = MailSender(subject, to, cc)
        sender.add_html(body)
        for fl in files:
            sender.add_file(fl)
        sender.send()


class MailReceiver:
    """ 邮件接收工具 """

    def __init__(self, section=Section.MAIL):
        assert section, "Mail Receiver Section Can't be empty!"
        self.cfg = Models.mapping(MailConfig, section)
        self._login_server()

    def _login_server(self):
        try:
            # 先查看端口再连接到POP3服务器
            telnetlib.Telnet(self.cfg.host, self.cfg.port_pop3)
            self.server = poplib.POP3(self.cfg.host, self.cfg.port_pop3, timeout=10)
            # 打印POP3服务器的欢迎文字:
            print(f"connect pop3 server success! {self.server.getwelcome().decode('utf-8')}")
            # server.set_debuglevel(1) # 可以打开或关闭调试信息
        except RuntimeError as ex:
            print(f"connect to mail pop3 server failed! {ex}")
            raise ex
        # 身份认证
        print(f"Login email server with user[{self.cfg.username}]")
        self.server.user(self.cfg.username)
        self.server.pass_(self.cfg.password)
        # 返回邮件数量和占用空间:
        print('Messages: %s. Size: %s' % self.server.stat())

    @staticmethod
    def _get_charset(msg):
        """获得msg的编码"""
        charset = msg.get_charset()
        if charset is None:
            content_type = msg.get('Content-Type', '').lower()
            pos = content_type.find('charset=')
            if pos >= 0:
                charset = content_type[pos + 8:].strip()
        return charset

    @staticmethod
    def _decode_str(str_in):
        """字符编码转换"""
        value, charset = decode_header(str_in)[0]
        if charset:
            value = value.decode(charset)
        return value

    def _read_content(self, msg):
        """获取邮件内容"""
        content_type = msg.get_content_type()
        if content_type in ('text/plain', 'text/html'):
            content = msg.get_payload(decode=True)
            charset = self._get_charset(msg)
            if charset:
                content = content.decode(charset)

            return content

    def _down_attach_files(self, msg_in, *, dir_path: TP.TPath = '', filter_keys: List[str] = None):
        """解析邮件,获取附件
        :param msg_in 邮件体
        :param dir_path 附件存放目录
        :param filter_keys 附件过滤关键字
        :return 附件全路径列表
        """
        attach_files = []
        if not dir_path:
            dir_path = Envs.DIR_CONF_ROOT.joinpath("mail", "attachments")
        if not Path.exists(dir_path):
            dir_path.mkdir(Consts.MASK755, True, True)

        for part in msg_in.walk():
            # 获取附件名称类型
            attach_name = part.get_filename()
            # contType = part.get_content_type()
            if attach_name:
                h = Header(attach_name)
                # 对附件名称进行解码
                dh = decode_header(h)
                filename = dh[0][0]
                if dh[0][1]:
                    # 将附件名称可读化
                    filename = self._decode_str(str(filename, dh[0][1]))

                # 默认下载附件
                tobe_down = any(filter(lambda k: filename.find(k) != -1, filter_keys)) if filter_keys else True

                # 根据关键字过滤结果下载附件
                if tobe_down:
                    data = part.get_payload(decode=True)
                    # 在指定目录下创建文件，注意二进制文件需要用wb模式打开
                    fl_path = Path.joinpath(dir_path, filename)
                    with open(fl_path, 'wb') as fl:
                        fl.write(data)
                    attach_files.append(fl_path)

        return attach_files

    def receive(self, obtain_date: TP.TDate = None, *, dir_path: TP.TPath = '', filter_keys: List[str] = None):
        """收邮件
        :param obtain_date 遍历邮件的截止时间. 默认只遍历当天.
        :param dir_path:Path 附件下载目录. 如果不存在会自动创建.
        :param filter_keys 附件中需要过滤的关键字, 多个key之间为or的关系
        """
        obtain_date = obtain_date if obtain_date and isinstance(obtain_date, datetime.date) else Dates.now()
        assert isinstance(obtain_date, dt.date), "obtain_date format error!"
        obtain_date = Dates.convert(obtain_date, DateFmt.Y_M_D)

        # list()返回所有邮件的编号:
        resp, mails, octets = self.server.list()
        # 可以查看返回的列表类似[b'1 82923', b'2 2184', ...]
        # print(mails)
        attach_files = []
        index = len(mails)
        for i in range(index, 0, -1):  # 倒序遍历邮件
            resp, lines, octets = self.server.retr(i)
            # lines存储了邮件的原始文本的每一行,
            # 邮件的原始文本:
            try:
                msg_content = b'\r\n'.join(lines).decode('utf-8')
            except UnicodeDecodeError:
                msg_content = b'\r\n'.join(lines).decode('gbk')

            # 解析邮件:
            msg = Parser().parsestr(msg_content)
            # 获取邮件的发件人，收件人， 抄送人,主题
            From = parseaddr(msg.get('from'))[1]
            To = parseaddr(msg.get('To'))[1]
            Cc = parseaddr(msg.get_all('Cc'))[1]
            Subject = self._decode_str(msg.get('Subject'))

            # 获取邮件时间,格式化收件时间
            recv_date = time.strptime(msg.get("Date")[0:24], '%a, %d %b %Y %H:%M:%S')
            # 邮件时间格式转换
            recv_date = time.strftime("%Y-%m-%d", recv_date)

            # 只取当前日期的邮件
            if recv_date < obtain_date:
                break

            print(f"ReceiveDate:{recv_date}\tFrom:{From}\tTo:{To}\tCc:{Cc}\tSubject:{Subject}")
            # 下载附件
            attach_files += self._down_attach_files(msg, dir_path=dir_path, filter_keys=filter_keys)
            print(f"Attachment files: {attach_files}. Filter Keys: {filter_keys}")

        # 关闭连接
        self.server.quit()
        return attach_files
