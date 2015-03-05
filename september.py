import os
import re
import sys
import json
import os.path
import logging
import argparse
import subprocess
import configparser
from urllib.parse import urlparse


logger = logging.getLogger(__name__)


class IRepo(object):
    def clone(self, repo_url, repo_path):
        raise NotImplementedError()

    def pull(self, repo_path):
        raise NotImplementedError()

    def getTags(self, repo_path):
        raise NotImplementedError()

    def update(self, repo_path, rev_id):
        raise NotImplementedError()


class GitRepo(object):
    def clone(self, repo_url, repo_path):
        subprocess.check_call(['git', 'clone', repo_url, repo_path])

    def pull(self, repo_path):
        subprocess.check_call(['git', 'pull', 'origin', 'master'])

    def getTags(self, repo_path):
        output = subprocess.check_output(['git', 'show-ref', '--tags'])
        pat = re.compile(r'^(?P<id>[0-9a-f]+) (?P<tag>.+)$')
        for line in output.split('\n'):
            m = pat.match(line)
            if m:
                yield (m.group('tag'), m.group('id'))

    def update(self, repo_path, rev_id):
        rev_id = rev_id or 'master'
        subprocess.check_call(['git', 'checkout', rev_id])


class MercurialRepo(object):
    def clone(self, repo_url, repo_path):
        subprocess.check_call(['hg', 'clone', repo_url, repo_path])

    def pull(self, repo_path):
        subprocess.check_call(['hg', 'pull'],
                              stderr=subprocess.STDOUT)

    def getTags(self, repo_path):
        output = subprocess.check_output(
                'hg log -r "tag()" --template "{tags} {node}\\n"',
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                shell=True)
        pat = re.compile(r'^(?P<tag>.+) (?P<id>[0-9a-f]+)$')
        for line in output.split('\n'):
            m = pat.match(line)
            if m:
                yield (m.group('tag'), m.group('id'))

    def update(self, repo_path, rev_id):
        rev_id = rev_id or 'default'
        subprocess.check_call(['hg', 'update', rev_id],
                              stderr=subprocess.STDOUT)


repo_class = {
        'git': GitRepo,
        'hg': MercurialRepo}


def guess_repo_type(repo):
    # Parse repo as an URL: scheme://netloc/path;parameters?query#fragment
    scheme, netloc, path, params, query, fragment = urlparse(repo)
    if scheme == 'ssh':
        if netloc.startswith('git@'):
            return 'git'
        if netloc.startswith('hg@'):
            return 'hg'
    elif scheme == 'https':
        if path.endswith('.git'):
            return 'git'
    elif scheme == '' and netloc == '' and os.path.isdir(path):
        if os.path.isdir(os.path.join(path, '.git')):
            return 'git'
        if os.path.isdir(os.path.join(path, '.hg')):
            return 'hg'
    return None


def main():
    # Setup the argument parser.
    parser = argparse.ArgumentParser(
            prog='september',
            description=("An utility that goes back in time and does "
                         "something in the background."))
    parser.add_argument(
            'repo',
            help="The repository to observe and process")
    parser.add_argument(
            '-t', '--tmp-dir',
            help="The temporary directory in which to clone the repository.")
    parser.add_argument(
            '--scm',
            default='guess',
            choices=['guess', 'git', 'mercurial'],
            help="The type of source control system handling the repository.")
    parser.add_argument(
            '--config',
            help="The configuration file to use.")
    parser.add_argument(
            '--command',
            help="The command to run on each tag.")

    # Parse arguments, guess repo type.
    res = parser.parse_args()
    repo_type = res.scm
    if not repo_type or repo_type == 'guess':
        repo_type = guess_repo_type(res.repo)
        if not repo_type:
            logger.error("Can't guess the repository type. Please use the "
                         "--scm option to specify it.")
            sys.exit(1)
        if repo_type not in repo_class:
            logger.error("Unknown repository type: %s" % repo_type)
            sys.exit(1)

    # Create the repo handler.
    repo = repo_class[repo_type]()

    # Clone or update/checkout the repository in the temp directory.
    clone_dir = os.path.join(res.tmp_dir, 'clone')
    if not os.path.exists(clone_dir):
        logger.info("Cloning '%s' into: %s" % (res.repo, clone_dir))
        repo.clone(res.repo, clone_dir)
    else:
        os.chdir(clone_dir)
        logger.info("Pulling changes from '%s'." % res.repo)
        repo.update(res.repo, None)

    os.chdir(clone_dir)

    # Find the configuration file in the repository clone.
    config_file = res.config or os.path.join(clone_dir, '.september.yml')
    config = configparser.ConfigParser(interpolation=None)
    if os.path.exists(config_file):
        logger.info("Loading configuration file: %s" % config_file)
        config.read(config_file)

    if not config.has_section('september'):
        config.add_section('september')
    config_sec = config['september']
    if res.command:
        config_sec['command'] = res.command

    if not config.has_option('september', 'command'):
        logger.error("There is no 'command' configuration setting under the "
                     "'september' section, and no command was passed as an "
                     "option.")
        sys.exit(1)

    # Find the cache file in the temp directory.
    cache_file = os.path.join(res.tmp_dir, 'september.json')
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as fp:
            cache = json.load(fp)
    else:
        cache = {'tags': {}}

    # Update the cache: get any new/moved tags.
    first_tag = config_sec.get('first_tag', None)
    tag_pattern = config_sec.get('tag_pattern', None)
    tag_re = None
    if tag_pattern:
        tag_re = re.compile(tag_pattern)

    previous_tags = cache['tags']
    tags = repo.getTags(clone_dir)
    for t, i in tags:
        if not tag_re or tag_re.search(t):
            if t not in previous_tags:
                logger.info("Adding tag '%s'." % t)
                previous_tags[t] = {'id': i, 'processed': False}
            elif previous_tags[t]['id'] != i:
                logger.info("Moving tag '%s'." % t)
                previous_tags[t] = {'id': i, 'processed': False}

        if first_tag and first_tag == t:
            break

    logger.info("Updating cache file '%s'." % cache_file)
    with open(cache_file, 'w') as fp:
        json.dump(cache, fp)

    # Process tags!
    use_shell = config_sec.get('use_shell') in ['1', 'yes', 'true']
    for tn, ti in cache['tags'].items():
        if ti['processed']:
            logger.info("Skipping '%s'." % tn)
            continue

        logger.info("Updating repo to '%s'." % tn)
        repo.update(clone_dir, ti['id'])

        command = config_sec['command'] % {
                'rev_id': ti['id'],
                'root_dir': clone_dir,
                'tag': tn}
        logger.info("Running: %s" % command)
        subprocess.check_call(command, shell=use_shell)

        ti['processed'] = True
        with open(cache_file, 'w') as fp:
            json.dump(cache, fp)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    main()

