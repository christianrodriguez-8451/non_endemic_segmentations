#Inspired from Patrick F.'s ML Ops example
from setuptools import setup, find_packages

setup(name='non_endemic_segmentations',
      version='1.0.0',
      description='Package to create and maintain the Non-Endemic Segmentations supporting PRISM.',
      #Add Steven Martz?
      author='Christian Rodriguez',
      #Add Steven Martz?
      author_email='christian.rodriguez@8451.com',
      url='https://github.com/christianrodriguez-8451/non_endemic_segmentations.git',
      #packages=find_packages(),
      packages=["non_endemic_segmentations"],
      install_requires=[
          # Numeric
          'pandas',
          'thefuzz',

          # MySQL
          'cx_Oracle',
          'sqlalchemy',
          'sqlparse',

          # 84.51
          'cdadata',
          'ccds',
      ])