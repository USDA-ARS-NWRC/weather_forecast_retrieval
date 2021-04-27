.. highlight:: shell

============
Contributing
============

Contributions are welcome, and they are greatly appreciated! Every
little bit helps, and credit will always be given.

You can contribute in many ways:

Types of Contributions
----------------------

Report Bugs
~~~~~~~~~~~

Report bugs at https://github.com/scotthavens/weather_forecast_retrieval/issues.

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

Fix Bugs
~~~~~~~~

Look through the GitHub issues for bugs. Anything tagged with "bug"
and "help wanted" is open to whoever wants to implement it.

Implement Features
~~~~~~~~~~~~~~~~~~

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

Write Documentation
~~~~~~~~~~~~~~~~~~~

Weather Forecast Retrieval could always use more documentation, whether as part of the
official Weather Forecast Retrieval docs, in docstrings, or even on the web in blog posts,
articles, and such.

Submit Feedback
~~~~~~~~~~~~~~~

The best way to send feedback is to file an issue at https://github.com/scotthavens/weather_forecast_retrieval/issues.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

Get Started!
------------

Ready to contribute? Here's how to set up `weather_forecast_retrieval` for local development.

#. Fork the `weather_forecast_retrieval` repo on GitHub to your user and check
   out the repository locally.

#. Install your local copy into a virtualenv. Assuming you have virtualenvwrapper
   installed, this is how you set up your fork for local development::

    $ mkvirtualenv weather_forecast_retrieval
    $ cd weather_forecast_retrieval/
    $ python setup.py develop

#. Create a branch for local development::

    $ git checkout -b name-of-your-bugfix-or-feature

   Now you can make your changes locally.

#. When you're done making changes, check that your changes pass flake8 and
   all the tests::

    $ flake8 weather_forecast_retrieval tests
    $ python -m unittest discover

#. Commit your changes and push your branch to GitHub::

    $ git add .
    $ git commit -m "Your detailed description of your changes."
    $ git push origin name-of-your-bugfix-or-feature

#. Submit a pull request through the GitHub website.

Pull Request Guidelines
-----------------------

Before you submit a pull request, check that it meets these guidelines:

#. The pull request should include tests.
#. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.rst.
#. Once opened, every pull request will be tested against every supported
   Python version through GitHub actions. Check the tab if there are any issues
   and that all workflows pass.

Releasing to PyPI
-----------------
To create a new release on `Pypi.org <https://pypi.org/>`_, follow these steps:

#. Create a new release for weather_forecast_retrieval
#. Name the tag and release the version number, for example `v0.7.0 <https://github.com/USDA-ARS-NWRC/weather_forecast_retrieval/releases/tag/v0.7.0>`_
#. Add documentation about the release and why it's different from the previous.
   Especially highlight any changes that will break existing integrations.
#. Publish new release which will trigger a build to release to PyPI

Tips
----

To run a subset of tests::


    $ python -m unittest tests.test_weather_forecast_retrieval


Skip running tests with external HTTP requests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To speed up development using local data, all tests that require an external
HTTP request can be skipped via environment variable. To use this option, set

::

    WFR_SKIP_EXTERNAL_REQUEST_TEST=1

as global variable in the environment that executes the tests.
