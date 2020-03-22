Ftrack Action for sending components from one location to another.

Requirements:

 - Python 3 with ftrack and accsyn API.s (pip install ftrack-python-api accsyn-pyton-api). (Compatible with Python 2 after changing catch expression @ line 362) 
 - FTRACK_ and ACCSYN_ environment variables to be set properly prior to invocation.
 - Custom locations created in Ftrack and Accsyn (site), with identical names.
 - Accsyn servers up and running, serving default root share on each site/location.
 - Components published with paths containing Ftrack project code, for example "P:\project\assets\render.geo". Use 'ftrack.unmanaged' location to prevent Ftrack from attempting to manage file handling. 
 - Assumes projects residing directly beneath Accsyn default root share.

Use/modify/distribute freely, at your own risk, no warranties or liabilities are provided. 

For more sample code, visit our GitHub: github.com/accsyn. Website: https://accsyn.com.
