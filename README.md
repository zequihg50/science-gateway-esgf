[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/zequihg50/science-gateway-esgf/HEAD?labpath=notebooks/getting-started.ipynb) [![IFCA](https://img.shields.io/badge/launch-IFCA-orange)](https://hub.climate4r.ifca.es/hub/user-redirect/git-pull?repo=https%3A%2F%2Fgithub.com%2Fzequihg50%2Fscience-gateway-esgf&urlpath=lab%2Ftree%2Fscience-gateway-esgf%2Fnotebooks%2Fgetting-started.ipynb)

# ESGF Virtual Aggregation Science Gateway

The ESGF Virtual Aggregation Science Gateway provides Virtual Analysis Ready Data (virtual ARD) for datasets hosted in the ESGF, based on the [ESGF Virtual Aggregation](https://doi.org/10.5194/gmd-18-2461-2025).

## Getting started

Run the science gateway by cliking on the launchers available. The [Binder launcher](https://mybinder.org/v2/gh/zequihg50/science-gateway-esgf/HEAD?labpath=notebooks/getting-started.ipynb) allows to run the science gateway on free computational resources provided by [MyBinder.org](https://mybinder.org). The IFCA launcher is only accesible to users registered in the IFCA Hub.

The [getting started notebook](notebooks/getting-started.ipynb) provides an overview of the science gateway and how to use it. Further notebooks are available in the [notebooks](notebooks) directory.

![Snapshot of the ESGF Virtual Aggregation Science Gateway](img/jupyter-lab.png)

## Notebooks

The notebooks might be run from any computer system. For a custom installation, refer to the required packages in the [environment.yml](environment.yml) file.

- [Getting started](notebooks/getting-started.ipynb) - Introduction and overview of the science gateway and its capabilities.
- [Model member agreement](notebooks/model-member-agreement.ipynb) - Ilustrates a workflow of model member agreement, based on model member democracy.
- [Analysis-Ready Cloud-Optimized](notebooks/arco.ipynb) - Illustrates how to generate ARCO datasets from the science gateway.
- [Niño 3.4 Index](notebooks/enso.ipynb) - Illustrates the calculation of Niño 3.4 index from the science gateway.
