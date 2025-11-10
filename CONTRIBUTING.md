Contributing Guide
==================

Thank you for considering a contribution to this project.  Contributions of any kind are welcome, including bug fixes, feature improvements, documentation, dashboards or model enhancements.

Workflow
--------

1. Fork and clone the repository.

   Use the GitHub fork button to create your own copy of the repository, then clone it locally:

   ```bash
   git clone https://github.com/your-username/retail-analytics-forecasting-platform.git
   cd retail-analytics-forecasting-platform
   ```

2. Create a topic branch.

   Work on a new branch named after the feature or fix you plan to implement:

   ```bash
   git checkout -b feature/my-feature
   ```

3. Install dependencies.

   Set up a virtual environment and install all required packages:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

   If your change touches the ETL scripts, ensure that PostgreSQL is available and `.env` is configured accordingly.

4. Make changes.

   - Keep your changes focused.  Separate unrelated fixes into different branches and pull requests.
   - Follow the existing coding style (PEP 8 for Python) and maintain docstrings and comments where appropriate.
   - When adding new notebooks, keep cells clean and runnable from top to bottom.

5. Run the pipeline and tests.

   After making changes, run the ETL scripts and the notebooks affected by your changes to ensure they still work.  Add or update unit tests if present in the repository.

6. Commit and push.

   Write clear, concise commit messages describing what your change does.  Push your branch to your fork:

   ```bash
   git add .
   git commit -m "Add feature X to transform_stage"
   git push origin feature/my-feature
   ```

7. Open a pull request.

   Go to the original repository and open a pull request from your branch.  Provide a description of the change, the motivation, and any relevant context.  If your change relates to an open issue, reference it in your pull request body (for example, `Closes #123`).

8. Respond to feedback.

   Repository maintainers may review your pull request and ask for changes.  Address feedback by pushing additional commits to your branch.  Once approvals are given, a maintainer will merge your changes.

Code of Conduct
---------------

Please be respectful and constructive in all interactions.  Harassment, discrimination or disrespectful behaviour will not be tolerated.
