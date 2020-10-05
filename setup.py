import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()



setuptools.setup(
  name = 'orchestra',
  version = '1.0',
  license='GPL-3.0',
  description = '',
  long_description = long_description,
  long_description_content_type="text/markdown",
  packages=setuptools.find_packages(),
  author = 'João Victor da Fonseca Pinto',
  author_email = 'jodafons@lps.ufrj.br',
  url = 'https://github.com/jodafons/orchestra',
  keywords = [],
  install_requires=[
          'Flask_Security',
          'passlib',
          'PTable',
          'Flask_Mail',
          'numpy',
          'requests',
          'Werkzeug',
          'SQLAlchemy',
          'Jinja2',
          'Flask',
          'Flask_Login',
          'Flask_SQLAlchemy',
          'BeneDict',
          'flask_admin',
          'flask_bootstrap',
          'flask_cors',
          'flask_restful',
          'Gaugi',
          'prettytable',
          'sqlalchemy_utils',
          'psycopg2-binary',
          'prometheus_client',
          'ansi2html',
      ],
  scripts=['scripts/maestro.py', 'scripts/run_orchestra.py'],
  classifiers=[
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'Topic :: Software Development :: Libraries :: Python Modules',
    'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
  ],
)
