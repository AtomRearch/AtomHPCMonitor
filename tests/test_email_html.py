import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from atomhpcmonitor.email_html import document, text_email
from atomhpcmonitor.mail import credentials, send, DeliveryError
from atomhpcmonitor.store import Store


class EmailTests(unittest.TestCase):
    def test_html_escapes_untrusted_evidence(self):
        text = '<script>alert("x")</script> & <img src=x>'
        html = text_email(text, text)
        self.assertNotIn('<script>', html)
        self.assertNotIn('<img', html)
        self.assertIn('&lt;script&gt;', html)
        self.assertIn('&amp;', html)
        self.assertNotIn('https://', html)

    def test_private_config_and_environment_override(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'smtp.local.json'
            path.write_text(json.dumps({'sender': 'file@example.com', 'password': 'private', 'receiver': 'file@example.com'}))
            settings = {'credentials_file': str(path)}
            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(credentials(settings)['password'], 'private')
                with patch.dict(os.environ, {'ATOMHPC_SMTP_SENDER': 'env@example.com'}):
                    self.assertEqual(credentials(settings)['sender'], 'env@example.com')
                    self.assertEqual(credentials(settings)['receiver'], 'file@example.com')
            path.write_text('{ malformed SECRET')
            with self.assertRaises(DeliveryError) as caught:
                credentials(settings)
            self.assertNotIn('SECRET', str(caught.exception))

    def test_mime_contains_plain_text_and_html(self):
        captured = []
        class SMTP:
            def __init__(self, *args, **kwargs): pass
            def login(self, *args): pass
            def send_message(self, message, **kwargs): captured.append(message); return {}
            def quit(self): pass
        env = {'ATOMHPC_SMTP_SENDER': 'test@example.com', 'ATOMHPC_SMTP_PASSWORD': 'private', 'ATOMHPC_SMTP_RECEIVER': 'receiver@example.com'}
        row = {'id': 1, 'account': 'hpc_user', 'subject': '计算进展', 'body': 'plain evidence', 'html_body': document('计算进展', 'hpc_user', '<p>HTML evidence</p>', 'footer')}
        with patch.dict(os.environ, env):
            send(row, {}, SMTP)
        message = captured[0]
        self.assertEqual(message.get_content_type(), 'multipart/alternative')
        self.assertEqual([p.get_content_type() for p in message.iter_parts()], ['text/plain', 'text/html'])
        self.assertIn('plain evidence', message.get_body(preferencelist=('plain',)).get_content())
        self.assertIn('HTML evidence', message.get_body(preferencelist=('html',)).get_content())
        self.assertNotIn('private', message.as_string())

    def test_existing_database_migrates_and_keeps_mail(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'state.db'
            db = sqlite3.connect(path)
            db.execute('CREATE TABLE outbox(id INTEGER PRIMARY KEY,event_id INTEGER NOT NULL UNIQUE,account TEXT NOT NULL,status TEXT NOT NULL DEFAULT "pending",attempts INTEGER NOT NULL DEFAULT 0,next_try REAL NOT NULL DEFAULT 0,updated REAL NOT NULL,error TEXT,subject TEXT NOT NULL,body TEXT NOT NULL)')
            db.execute('INSERT INTO outbox(event_id,account,updated,subject,body) VALUES(100,"hpc_user",0,"old","evidence")')
            db.commit(); db.close()
            store = Store(path)
            self.assertEqual(store.outbox('hpc_user')[0]['body'], 'evidence')
            self.assertIsNone(store.outbox('hpc_user')[0]['html_body'])
            store.enqueue_report('hpc_user', 'new', 'new', 'new', {}, 1, '<p>saved</p>')
            store.close()
            store = Store(path)
            self.assertEqual(store.outbox('hpc_user')[1]['html_body'], '<p>saved</p>')
            store.close()


if __name__ == '__main__':
    unittest.main()
