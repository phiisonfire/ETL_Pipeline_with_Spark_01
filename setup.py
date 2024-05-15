from setuptools import find_packages, setup
from typing import List
# find_packages automatically find out all the packages that are available in the entire project

HYPEN_E_DOT = '-e .'

def get_requirements(file_path:str)->List[str]:
    """
    This function will return the list of requirements. 
    """
    requirements = []
    with open(file_path) as file_object:
        requirements = file_object.readlines()
        requirements = [req.replace('\n', '') for req in requirements]

        if HYPEN_E_DOT in requirements:
            requirements.remove(HYPEN_E_DOT)
    
    return requirements
# metadata information for the entire project
setup(
    name='ETL_Pipeline_with_Spark',
    version='0.1',
    author='Phi Nguyen',
    author_email='nguyenhongphi2807@gmail.com',
    packages=find_packages(),
    install_requires=get_requirements('requirements.txt')
)