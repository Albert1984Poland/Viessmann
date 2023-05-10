

# Build and deploy a package
1. Install wheel package
```
    python -m pip install -U pip wheel setuptools
```
2.Go to working directory "..\Repositories\Viessmann" and execute:
```
    python setup.py bdist_wheel
```
3. This will produce wheel package in dist folder.
4. Check the wheel package
```
    twine check dist/*
```
5. Load package to the pypi repository
```
    twine upload -r testpypi dist/*
```
6. Install package on cluster as PyPI
```
   Package: viessmann-lib-pkg==1.0.0 
   URL: https://test.pypi.org/project/

```
