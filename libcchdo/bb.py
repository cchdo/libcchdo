from logging import getLogger
from socket import error as sockerr
from SimpleHTTPServer import SimpleHTTPRequestHandler
from urlparse import urlparse, parse_qsl
from webbrowser import open as webopen

import oauth2 as oauth

from libcchdo.serve import get_local_host, open_server_on_high_port
from libcchdo.config import get_option, set_option, ConfigError


log = getLogger(__name__)


class AuthenticatorHTTPServer(SimpleHTTPRequestHandler):
    def do_GET(self):
        params = dict(parse_qsl(self.path[2:]))
        self.send_response(200, 'OK')
        self.send_header('Content-type', 'text/html')
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(
            '<title>Authorized libcchdo for Bitbucket!</title>'
            '<h1>Authorized libcchdo for Bitbucket!</h1>'
            '<p>Close this window at any time.</p>')
        try:
            self.server.oauth_verifier = params['oauth_verifier']
        except KeyError:
            pass


class BBOAuth(object):
    consumer_key = 'gWRCWSpSQAJNTfXR5d'
    consumer_secret = 'nLLKZe8dkRnJTd4qf4YpDWvsT9JFZtge'

    def get_consumer(self):
        return oauth.Consumer(self.consumer_key, self.consumer_secret)

    def authorize(self):
        """Perform full OAuth authentication and authorization.
        
        Return:
        dict containing oauth_token and oauth_token_secret for access_token.

        """
        host = get_local_host('bitbucket.org')
        httpd, port = open_server_on_high_port(AuthenticatorHTTPServer)

        oauth_api_uri = 'https://bitbucket.org/!api/1.0/oauth'
        oauth_callback = 'http://{host}:{port}'.format(host=host, port=port)
        request_token_url = (
            '{0}/request_token?oauth_callback={oauth_callback}').format(
            oauth_api_uri, oauth_callback=oauth_callback)
        authorize_url = '{0}/authenticate'.format(oauth_api_uri)
        access_token_url = '{0}/access_token'.format(oauth_api_uri)

        consumer = self.get_consumer()
        client = oauth.Client(consumer)

        resp, content = client.request(request_token_url, 'POST')
        if resp['status'] != '200':
            print resp
            raise Exception('Invalid response {0}.'.format(resp['status']))

        request_token = dict(parse_qsl(content))

        # Step 2: Send user to the provider to authenticated and authorize.
        authorize_url = '{0}?oauth_token={1}'.format(
            authorize_url, request_token['oauth_token'])

        print "Visit in your browser and click Grant Access:"
        print authorize_url

        webopen(authorize_url)
        httpd.handle_request()

        token = oauth.Token(
            request_token['oauth_token'], request_token['oauth_token_secret'])
        try:
            token.set_verifier(httpd.oauth_verifier)
        except AttributeError:
            log.error(u'Did not get OAuth verifier.')
            return
        client = oauth.Client(consumer, token)

        resp, content = client.request(access_token_url, "POST")
        if resp['status'] != '200':
            log.error(u'Unable to get access token.')
            log.debug(resp)
            log.debug(content)
            return
        access_token = dict(parse_qsl(content))
        return (access_token['oauth_token'],
                access_token['oauth_token_secret'])

    def get_access_token(self):
        try:
            oauth_token = get_option('bb_oauth', 'oauth_token')
            oauth_token_secret = get_option('bb_oauth', 'oauth_token_secret')
        except ConfigError:
            oauth_token, oauth_token_secret = self.authorize()
            set_option('bb_oauth', 'oauth_token', oauth_token)
            set_option('bb_oauth', 'oauth_token_secret', oauth_token_secret)

        try:
            token = oauth.Token(oauth_token, oauth_token_secret)
        except KeyError:
            log.error(u'Unable to get access token.')
            return None
        return token

    def client(self):
        return oauth.Client(self.get_consumer(), self.get_access_token())

    def api(self, method, path):
        client = self.client()
        api_base = 'https://api.bitbucket.org/1.0'
        api_uri = api_base + path
        return client.request(api_uri, 'GET')


BB = BBOAuth()
