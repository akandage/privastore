import uuid
import unittest
from .session import Sessions

class TestSession(unittest.TestCase):
    
    def setUp(self):
        self.sessions = Sessions()

    def tearDown(self):
        pass

    def check_session_id(self, session_id):
        if not session_id.startswith('S-'):
            return False 
        try:
            uuid.UUID(session_id[2:])
        except:
            return False
        return True

    def test_start_session(self):
        sess1 = self.sessions.start_session('testuser')
        sess2 = self.sessions.start_session('testuser')
        sess3 = 'S-' + str(uuid.uuid4())

        self.assertTrue(self.check_session_id(sess1), 'Invalid session id 1')
        self.assertTrue(self.check_session_id(sess2), 'Invalid session id 2')
        self.assertNotEqual(sess1, sess2, 'Session id\'s not unique')
        self.assertTrue(self.sessions.is_valid_session(sess1), 'Session 1 invalid')
        self.assertTrue(self.sessions.is_valid_session(sess2), 'Session 2 invalid')
        self.assertFalse(self.sessions.is_valid_session(sess3), 'Session 3 valid')
    
    def test_get_session_user(self):
        sess1 = self.sessions.start_session('testuser1')
        sess2 = self.sessions.start_session('testuser2')
        sess3 = 'S-' + str(uuid.uuid4())

        self.assertEqual(self.sessions.get_session_user(sess1), 'testuser1', 'Incorrect session user')
        self.assertEqual(self.sessions.get_session_user(sess2), 'testuser2', 'Incorrect session user')

        try:
            self.sessions.get_session_user(sess3)
            self.fail()
        except Exception as e:
            self.assertEqual(str(e), 'Session id [{}] not found!'.format(sess3), 'Expected session id not found')

    def test_renew_session(self):
        sess1 = self.sessions.start_session('testuser')
        # Expired session.
        sess2 = self.sessions.start_session('testuser', -1)

        self.assertTrue(self.sessions.is_valid_session(sess1), 'Session 1 invalid')
        self.assertFalse(self.sessions.is_valid_session(sess2), 'Session 2 valid')
        self.sessions.renew_session(sess2)
        self.assertTrue(self.sessions.is_valid_session(sess1), 'Session 1 invalid')
        self.assertTrue(self.sessions.is_valid_session(sess2), 'Session 2 invalid')
    
    def test_renew_invalid_session(self):
        sess = 'S-' + str(uuid.uuid4())
        sess1 = self.sessions.start_session('testuser')
        self.sessions.renew_session(sess1)
        try:
            self.sessions.renew_session(sess)
            self.fail()
        except Exception as e:
            self.assertEqual(str(e), 'Session id [{}] not found!'.format(sess), 'Expected session id not found')
    
    def test_end_session(self):
        sess1 = self.sessions.start_session('testuser')
        sess2 = self.sessions.start_session('testuser')

        self.assertTrue(self.sessions.is_valid_session(sess1), 'Session 1 invalid')
        self.assertTrue(self.sessions.is_valid_session(sess2), 'Session 2 invalid')
        self.sessions.end_session(sess1)
        self.assertFalse(self.sessions.is_valid_session(sess1), 'Session 1 valid')
        self.assertTrue(self.sessions.is_valid_session(sess2), 'Session 2 invalid')

        try:
            self.sessions.end_session(sess1)
            self.fail()
        except Exception as e:
            self.assertEqual(str(e), 'Session id [{}] not found!'.format(sess1), 'Expected session id not found')

    def test_end_invalid_session(self):
        sess = 'S-' + str(uuid.uuid4())
        sess1 = self.sessions.start_session('testuser')
        try:
            self.sessions.end_session(sess)
            self.fail()
        except Exception as e:
            self.assertEqual(str(e), 'Session id [{}] not found!'.format(sess), 'Expected session id not found')
        self.sessions.end_session(sess1)
    
    def test_remove_expired_sessions(self):
        sess1 = self.sessions.start_session('testuser1')
        sess2 = self.sessions.start_session('testuser2')

        sessions_removed = self.sessions.remove_expired_sessions()
        self.assertEqual(sessions_removed, 0, 'Expected no sessions removed')
        sess3 = self.sessions.start_session('testuser3', -1)
        sess4 = self.sessions.start_session('testuser4')
        sess5 = self.sessions.start_session('testuser3', -1)
        sessions_removed = self.sessions.remove_expired_sessions()
        self.assertEqual(sessions_removed, 2, 'Expected 2 sessions removed')
        self.assertTrue(self.sessions.is_valid_session(sess1), 'Session 1 removed')
        self.assertTrue(self.sessions.is_valid_session(sess2), 'Session 2 removed')
        self.assertFalse(self.sessions.is_valid_session(sess3), 'Session 3 not removed')
        self.assertTrue(self.sessions.is_valid_session(sess4), 'Session 4 removed')
        self.assertFalse(self.sessions.is_valid_session(sess5), 'Session 5 not removed')