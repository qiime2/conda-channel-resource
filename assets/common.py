import os
import io
import bz2
import json
import shutil
import ftplib
import subprocess
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
        self._repodata[spec['subdir']][filename] = spec

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
                raise Exception("anaconda login failed")
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
        files_to_upload = [
            os.path.join(data.root, relpath) for relpath in relpaths]
        subprocess.run('anaconda upload --force -u %r %s'
                       % (self._channel, ' '.join(files_to_upload)),
                       shell=True, check=True, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
        return relpaths


class FTPConnection:
    def __init__(self, uri, channel, username, password):
        host = urllib.parse.urlsplit(uri).netloc
        if ':' in host:
            source_address = host.split(':')
            self._ftp = ftplib.FTP(user=username, passwd=password,
                                   source_address=source_address)
        else:
            self._ftp = ftplib.FTP(host, username, password)
        self._ftp.cwd(channel)

    def __enter__(self):
        self._ftp.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._ftp.__exit__(exc_type, exc_value, traceback)

    def download(self, path, filehandle):
        try:
            self._ftp.retrbinary('RETR %s' % path, filehandle.write)
        except ftplib.error_perm:
            raise FileNotFoundError(path)

    def upload_local_data(self, data, name, version):
        new_data = ChannelData(conn=self)
        relpaths = list(data.iter_paths(name=name, version=version))

        for filename, spec in data.iter_entries(name=name, version=version):
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
        return FTPConnection(uri, channel, username, password)
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
