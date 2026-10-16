"""SMTP_SSL with explicit accepted / failed / unknown delivery outcomes."""
import os
import json
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import formatdate
from pathlib import Path
from .email_html import text_email


class DeliveryError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def credentials(settings):
    values = {}
    path = settings.get("credentials_file")
    if path and Path(path).is_file():
        try:
            values = json.loads(Path(path).read_text(encoding="utf-8-sig"))
            if not isinstance(values, dict):
                raise ValueError("expected object")
        except (OSError, ValueError):
            raise DeliveryError("failed", "SMTP 私有配置无法读取或格式错误；凭据未写入日志") from None
    result = {}
    for key, default in (("sender", "ATOMHPC_SMTP_SENDER"), ("password", "ATOMHPC_SMTP_PASSWORD"), ("receiver", "ATOMHPC_SMTP_RECEIVER")):
        result[key] = os.environ.get(settings.get(key + "_env", default)) or values.get(key)
        if result[key] is not None and (not isinstance(result[key], str) or "\n" in result[key] or "\r" in result[key]):
            raise DeliveryError("failed", "SMTP 凭据字段必须为单行字符串；凭据未写入日志")
    return result


def send(row, settings, factory=smtplib.SMTP_SSL):
    values = credentials(settings)
    sender, password, receiver = values["sender"], values["password"], values["receiver"]
    if not sender or not password or not receiver:
        raise DeliveryError("failed", "SMTP 环境变量或 smtp.local.json 未配置完整（凭据不会写入日志）")
    message = EmailMessage()
    message["From"] = sender
    message["To"] = receiver
    message["Subject"] = row["subject"]
    message["Date"] = formatdate(localtime=False)
    message["Message-ID"] = f"<atomhpc-{row['account']}-{row['id']}@{sender.split('@')[-1]}>"
    message.set_content(row["body"])
    message.add_alternative(row.get("html_body") or text_email(row["subject"], row["body"]), subtype="html")
    server = None
    phase = "connect"
    try:
        server = factory(settings.get("host", "smtp.qq.com"), int(settings.get("port", 465)),
                         timeout=float(settings.get("timeout", 30)), context=ssl.create_default_context())
        server.login(sender, password)
        phase = "data"
        refused = server.send_message(message, from_addr=sender, to_addrs=[receiver])
        if refused:
            raise DeliveryError("failed", "SMTP 明确拒绝收件人")
        # A successful DATA response means accepted even if QUIT later fails.
        phase = "accepted"
    except DeliveryError:
        raise
    except (smtplib.SMTPRecipientsRefused, smtplib.SMTPSenderRefused,
            smtplib.SMTPDataError, smtplib.SMTPAuthenticationError) as exc:
        raise DeliveryError("failed", f"SMTP 明确拒绝（{type(exc).__name__}）；请检查邮箱配置") from None
    except (OSError, smtplib.SMTPException, ValueError) as exc:
        status = "unknown" if phase == "data" else "failed"
        raise DeliveryError(status, f"SMTP {phase} 阶段异常（{type(exc).__name__}）") from None
    finally:
        if server:
            try:
                server.quit()
            except (OSError, smtplib.SMTPException):
                try:
                    server.close()
                except OSError:
                    pass
    return "accepted"


def deliver(store, account, settings, now, sender=send, only_ids=None):
    store.recover_mail(account, now)
    outcomes = []
    if not settings.get("enabled", False):
        return outcomes
    for row in store.outbox(account):
        if only_ids is not None and row["id"] not in only_ids:
            continue
        if row["status"] not in ("pending", "failed") or row["next_try"] > now:
            continue
        store.mail_state(row["id"], "sending", now, increment=True)
        try:
            sender(row, settings)
            status, error, retry = "accepted", None, 0
        except DeliveryError as exc:
            status, error = exc.status, str(exc)
            retry = now + min(86400, 60 * 2 ** min(row["attempts"], 10)) if status == "failed" else 0
        store.mail_state(row["id"], status, now, error, retry)
        outcomes.append({"id": row["id"], "status": status, "error": error})
    return outcomes
