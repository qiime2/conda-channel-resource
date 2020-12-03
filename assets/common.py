import os
import io
import bz2
import json
import time
import shutil
import ftplib
import tempfile
import subprocess
import contextlib
import urllib.parse
import urllib.request

# Example Configuration
# ---------------------
# source:
#     pkg_name: awesome-package
#     uri: ftp://example.com
#     channel: some_directory
#     user: foo
#     pass: bar
#
# source:
#     pkg_name: awesome-package
#     uri: https://conda.anaconda.org
#     channel: example
#     user: foo
#     pass: bar


class ChannelData:
    SUBDIRS = ['noarch', 'osx-64', 'linux-64', 'linux-32', 'win-64', 'win-32']
    REPODATA = 'repodata.json'
    ZIP_EXT = '.bz2'

    def __init__(self, conn=None, path=None):
        if conn is None and path is None:
            raise Exception('Missing a source.')
        if conn is not None and path is not None:
            raise Exception('Multiple sources.')

        self._root = None
        if path is not None:
            self._root = path

        self._repodata = {}
        for subdir in self.SUBDIRS:
            repodata_path = os.path.join(subdir, self.REPODATA)
            try:
                if conn is not None:
                    with io.TextIOWrapper(io.BytesIO()) as fh:
                        conn.download(repodata_path, fh.buffer)
                        fh.seek(0)
                        self._repodata[subdir] = json.load(fh)
                else:
                    with open(os.path.join(self._root, repodata_path),
                              mode='r') as fh:
                        self._repodata[subdir] = json.load(fh)
            except FileNotFoundError:
                self._repodata[subdir] = {'info': {}, 'packages': {}}

    @property
    def root(self):
        if self._root is None:
            raise Exception("Not locally sourced channel data, no root.")
        return self._root

    def add(self, filename, spec):
        self._repodata[spec['subdir']]['packages'][filename] = spec

    def iter_repodata_filehandles(self):
        for subdir, repodata in self._repodata.items():
            if not repodata['packages']:
                continue
            relpath = os.path.join(subdir, self.REPODATA)
            contents = json.dumps(repodata).encode('utf-8')
            yield relpath, io.BytesIO(contents)
            yield relpath + self.ZIP_EXT, io.BytesIO(bz2.compress(contents))

    def iter_entries(self, name=None, version=None):
        for repodata in self._repodata.values():
            for filename, spec in repodata['packages'].items():
                if name is not None and name != spec['name']:
                    continue
                if version is not None and version != spec['version']:
                    continue
                yield filename, spec

    def iter_paths(self, name=None, version=None):
        for filename, spec in self.iter_entries(name=name, version=version):
            yield os.path.join(spec['subdir'], filename)

    def get_names(self):
        return {spec['name'] for _, spec in self.iter_entries()}

    def get_versions(self, name):
        return {spec['version'] for _, spec in self.iter_entries(name=name)}


class AnacondaConnection:
    URI = 'https://conda.anaconda.org'

    def __init__(self, channel, username, password):
        self._channel = channel
        self._user = channel.split('/')[0]
        self._label = 'main'
        if 'label' in channel:
            self._label = channel.split('/')[-1]
        self._username = username
        self._password = password

    def __enter__(self):
        if self._username:
            try:
                subprocess.run('anaconda login --username %r --password %r'
                               % (self._username, self._password),
                               shell=True, check=True,
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
            # Catch exit code exception so that the username and password
            # aren't shown in stacktrace
            except subprocess.CalledProcessError:
                raise Exception("anaconda login failed") from None
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        subprocess.run('anaconda logout',
                       shell=True, check=True, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)

    def download(self, path, filehandle):
        url = os.path.join(self.URI, self._channel, path)
        fh = urllib.request.urlopen(url)
        shutil.copyfileobj(fh, filehandle)

    def upload_local_data(self, data, name, version):
        relpaths = list(data.iter_paths(name=name, version=version))
        files = ' '.join(repr(os.path.join(data.root, relpath))
                         for relpath in relpaths)
        with tempfile.TemporaryFile('w+') as stdout, \
                tempfile.TemporaryFile('w+') as stderr:
            try:
                subprocess.run('anaconda upload -u %r -l %r %s'
                               % (self._user, self._label, files), shell=True,
                               check=True, stdout=stdout, stderr=stderr)
            except Exception:
                stdout.seek(0)
                stderr.seek(0)
                print(stdout.read())
                print(stderr.read())
                raise

        return relpaths


@contextlib.contextmanager
def ftp_lock(ftp):
    LOCKD = '.lock'
    start = time.time()
    locked = False
    while not locked:
        try:
            ftp.mkd(LOCKD)
            locked = True
        except ftplib.error_perm:
            # directory exists, so the resource is already locked
            time.sleep(5)
            if time.time() - start > 5 * 60:
                raise Exception("Could not acquire '.lock'. Is it stale?")
    try:
        yield
    finally:
        ftp.rmd(LOCKD)


class FTPConnection:
    def __init__(self, uri, channel, username, password, tls):
        channel = str(channel)
        host = urllib.parse.urlsplit(uri).netloc
        FTPClass = ftplib.FTP_TLS if tls else ftplib.FTP
        if ':' in host:
            host, port = host.split(':')
            self._ftp = FTPClass()
            self._ftp.connect(host, port=int(port))
            self._ftp.login(user=username, passwd=password)
        else:
            self._ftp = FTPClass(host, username, password)

        try:
            self._ftp.cwd(channel)
        except ftplib.error_perm:
            self._mkpath(channel)
            self._ftp.cwd(channel)

    def __enter__(self):
        self._ftp.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._ftp.__exit__(exc_type, exc_value, traceback)

    def _mkpath(self, path):
        existing_path = []
        for segment in path.split('/'):
            existing_path.append(segment)
            try:
                self._ftp.mkd('/'.join(existing_path))
            except ftplib.error_perm:
                pass

    def download(self, path, filehandle):
        try:
            self._ftp.retrbinary('RETR %s' % path, filehandle.write)
        except ftplib.error_perm:
            raise FileNotFoundError(path)

    def upload_local_data(self, data, name, version):
        with ftp_lock(self._ftp):
            new_data = ChannelData(conn=self)
            relpaths = list(data.iter_paths(name=name, version=version))

            for filename, spec in data.iter_entries(name=name,
                                                    version=version):
                new_data.add(filename, spec)

            for relpath in relpaths:
                try:
                    self._ftp.mkd(os.path.dirname(relpath))
                except ftplib.error_perm:
                    pass  # directory already exists
                with open(os.path.join(data.root, relpath), mode='rb') as fh:
                    self._ftp.storbinary('STOR %s' % relpath, fh)

            for relpath, fh in new_data.iter_repodata_filehandles():
                self._ftp.storbinary('STOR %s' % relpath, fh)
                fh.close()

        return relpaths


def connect(source):
    source = source.copy()
    username = source.pop('user', "")
    password = source.pop('pass', "")
    source.pop('regex', None)
    try:
        source.pop('pkg_name')
        uri = source.pop('uri')
        channel = source.pop('channel')
    except KeyError as e:
        raise Exception("Missing source configuration: %r" % e.args[0])

    if source:
        raise Exception("Unknown source keys: %r" % set(source))

    if uri == AnacondaConnection.URI:
        return AnacondaConnection(channel, username, password)
    elif uri.startswith('ftp://'):
        return FTPConnection(uri, channel, username, password, tls=False)
    elif uri.startswith('ftps://'):
        return FTPConnection(uri, channel, username, password, tls=True)
    else:
        raise Exception("Unknown URI: %r" % uri)


def to_version(version_string):
    if type(version_string) is not str:
        raise Exception("Version spec violated, expected string, got: %r"
                        % (version_string,))
    return {'version': version_string}


def from_version(version_spec):
    if version_spec is None:
        return None
    try:
        return version_spec['version']
    except KeyError:
        raise Exception("Version spec violated, expected key 'version', got: "
                        "%r" % (version_spec,))
