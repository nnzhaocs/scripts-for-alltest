
#from analysis_library import *

import re

def filter_whole_types(tstrs):
    # ELF and excutable,
    # words = re.split(' |; |, |\"|\" ', tstr)
    if len(tstrs):
        tstr = tstrs[0].replace('sticky ,', '').replace('sticky ', '').replace('setuid ', '').replace('setgid ', '').replace \
            ('setuid ', '')
    else:
        return 'non-type'

    words = re.split(' |; |, |\"|\" ', tstr)
    if 'ELF' in words:
        return filter_ELF_types(words)

    elif 'executable' in words:
        return filter_non_ELF_executable_types(words)

    elif 'relocatable' in words:
        return 'nonELF-relocatable'
    elif 'compiled' in words or 'Compiled' in words or 'byte-compiled' in words:
        return filter_compiled(words)
    elif 'library' in words:
        return filter_library(words)
    elif "precompiled" in words:
        return 'gcc-precompiled-header'
    elif 'RPM' in words and 'bin' in words:
        return 'RPM-bin-pak'
    elif 'Debian' in words and 'binary' in words:
        return 'debian-bin-pak'
    elif 'image' in words:
        return filter_image(words)
    elif 'document' in words:
        return filter_doc(words)
    elif 'c' in words or 'C' in words or 'C++' in words or 'c++' in words:
        return 'c-c--source'

    ret, find =  filter_database(words)
    if find:
        return ret
    else:
        ret, find = filter_archival(words)
        if find:
            return ret
        else:
            ret, find = filter_java(words)
            if find:
                return ret
            else:
                if 'font' in words:
                    return 'font-type'
                tstr = tstr.lower()
                # if 'version' in tstr:
                #    tstr = tstr.split('version')[0]
                # if 'at' in tstr:
                #    tstr = tstr.split('at')[0]
                # if 'with' in tstr:
                #    tstr = tstr.split('with')
                tstr = tstr.strip()
                if ',' in tstr:
                    if tstr.split(',')[0].strip() != '':
                        tstr = tstr.split(',')[0].strip()
                    else:
                        tstr = tstr.split(',')[1].strip()

                if 'version' in tstr:
                    tstr = tstr.split(' version ')[0].strip()
                if 'at' in tstr:
                    tstr = tstr.split(' at ')[0].strip()
                if 'with' in tstr:
                    tstr = tstr.split(' with ')[0].strip()
                if ':' in tstr or ';' in tstr or '(' in tstr:
                    words = re.split(':|;|\(| . | - ' ,tstr  )  # [0] != ' '#.split(',')[0]
                    if words[0].strip() != '':
                        return  words[0].strip()
                    else:
                        return words[1].strip()

                        # nospace_words = []
                        # for word in words:
                        #    new = word.replace(' ', '')
                        #    if new != '':
                        #        nospace_words.append(new)
                        # return nospace_words[0]
                else:
                    return tstr


def filter_java(words):
    lowcase_words = []
    for word in words:
        lowcase_words.append(word.lower())

    if 'java' in lowcase_words and 'keystore' in lowcase_words:
        return 'java-keystore', True
    elif 'java' in lowcase_words and 'serialization' in lowcase_words:
        return 'java-serialization', True
    else:
        return None, False


def filter_archival(words):
    lowcase_words = []
    for word in words:
        lowcase_words.append(word.lower())

    if 'zip' in lowcase_words:
        return 'zip-arch', True
    elif 'gzip' in lowcase_words:
        return 'gzip-arch', True
    elif 'xz' in lowcase_words:
        return 'xz-arch', True
    elif 'bzip2' in lowcase_words:
        return 'bzip2-arch', True
    elif 'tar' in lowcase_words:
        return 'tar-arch', True
    elif 'archive' in lowcase_words:
        return 'other-arch', True
    else:
        return None, False


def filter_database(words):
    lowcase_words = []
    for word in words:
        lowcase_words.append(word.lower())

    if 'berkeley' in lowcase_words and 'db' in lowcase_words:
        return 'berkely-db', True
    elif 'dbase' in lowcase_words:
        return 'dbase-db', True
    elif 'sqlite' in lowcase_words:
        return 'sqlite-db', True
    elif 'clam' in lowcase_words and 'antivirtus' in lowcase_words:
        return 'clam-anti-db', True
    elif 'ndbm' in lowcase_words and 'database' in lowcase_words:
        return 'ndbm-db', True
    elif 'database' in lowcase_words:
        return 'other-db', True
    else:
        return None, False



def filter_doc(words):
    lowcase_words = []
    for word in words:
        lowcase_words.append(word.lower())
    if 'html' in lowcase_words or 'xhtml' in lowcase_words or 'xml' in lowcase_words:
        return 'html-xml-xhtml-doc'
    elif 'latex' in lowcase_words or 'tex' in lowcase_words or 'bibtex' in lowcase_words:
        return 'latex-tex-bib-doc'
    elif 'postscript' in lowcase_words or 'pdf' in lowcase_words:
        return 'ps-pdf-doc'
    elif 'composite' in lowcase_words:
        return 'composite-doc'
    elif 'microsoft' in lowcase_words:
        return 'microsoft-doc'
    elif 'perl' in lowcase_words and 'pod' in lowcase_words:
        return 'perl-pod-doc'
    elif 'exported' in lowcase_words and 'sgml' in lowcase_words:
        return 'exported-sgml-doc'
    elif 'lyx' in lowcase_words:
        return 'lyx-doc'
    elif 'openoffice.org' in lowcase_words:
        return 'openoffice-doc'
    else:
        return 'other-doc'


def filter_image(words):
    if 'JPEG' in words:
        return 'jpeg-image'
    elif 'PNG' in words:
        return 'png-image'
    elif 'SVG' in words:
        return 'svg-image'
    elif 'TIFF' in words:
        return 'tiff-image'
    elif 'FIG' in words:
        return 'fig-image'
    elif 'FITS' in words:
        return 'fits-image'
    elif 'pixmap' in words and 'X' in words:
        return 'x-pixmap-image'
    elif 'VISX' in words:
        return 'visx-image'
    elif 'Photoshop' in words:
        return 'photoshop-image'
    else:
        return 'other-image'


def filter_compiled(words):
    lowcase_words = []
    for word in words:
        lowcase_words.append(word.lower())
    if 'python' in lowcase_words:
        return 'python byte-compiled'
    elif 'java' in lowcase_words:
        return 'compiled-java-class'
    elif 'emacs' in lowcase_words or 'xemacs' in lowcase_words or 'emacs/xemacs' in lowcase_words:
        return 'xemacs-emacs-compiled'
    elif 'terminfo' in lowcase_words:
        return 'terminfo-compiled'
    elif 'psi' in lowcase_words:
        return 'psi-compiled'
    else:
        return 'other-compiled'

def filter_library(words):
    lowcase_words = []
    for word in words:
        lowcase_words.append(word.lower())

    if 'libtool' in lowcase_words:
        return 'libtool-lib'
    elif 'ocaml' in lowcase_words:
        return 'ocaml-lib'
    elif 'palm' in lowcase_words:
        return 'palm-lib'
    elif 'mach-o' in lowcase_words:
        return 'mach-o-lib'
    else:
        return 'other-lib'


def filter_non_ELF_executable_types(words):
    # words = re.split(' |; |, |\"|\" ', tstr)
    lowcase_words = []
    for word in words:
        lowcase_words.append(word.lower())
    if 'script' in lowcase_words:
        return filter_script(lowcase_words)
    else:
        return filter_non_script(lowcase_words)


def filter_script(lowcase_word):
    if 'python' in lowcase_word:
        return "python-script"
    elif 'bash' in lowcase_word:
        return 'bash-shell-script'
    elif 'shell' in lowcase_word:
        return 'bash-shell-script'
    elif 'ruby' in lowcase_word:
        return 'ruby-jruby-script'
    elif 'jruby' in lowcase_word:
        return 'ruby-jruby-script'
    elif 'node' in lowcase_word:
        return  'node-script'
    elif 'perl' in lowcase_word:
        return 'perl-script'
    elif 'php' in lowcase_word:
        return 'php-script'
    else:
        return 'other-script'


def filter_non_script(lowcase_word):
    if 'pe32' in lowcase_word or 'pe32+' in lowcase_word:
        return 'PE-PE32-execu'
    elif 'vax'  in lowcase_word and 'coff' in lowcase_word:
        return 'VAX-COFF-execu'
    else:
        return 'other-nonELF-execu'


def filter_ELF_types(words):
    # words = re.split(' |; |, |\"|\" ', tstr)
    if 'relocatable' in words:
        return 'ELF-relocatable'
    elif 'shared' in words:
        return 'ELF-shared'
    elif 'core'  in words:
        return 'ELF-core'
    elif 'processor-specific' in words:
        return 'ELF-processor-specific'
    else:
        return 'ELF-others'
