# GitHub App Setup (Scaffold)

1. Create a GitHub App (Org or User level).
   - Permissions:
     - Repository contents: Read & Write
     - Pull requests: Read & Write
     - Checks: Read & Write
     - Commit statuses: Read & Write
   - Subscribe to events: pull_request, check_suite, check_run, push

2. Install the App on your repo.

3. In CI, authenticate as the App:
   - Use `actions/github-script` or a dedicated action to mint an installation token.
   - Use that token for branch protection and merges.

4. Protect `main`:
   - Require passing checks:
     - Policy and Math
     - Calibration and Backtest Gates
   - Require signed commits if you enable signing.

5. Remove PATs. Only the App should have merge rights.

This repo includes minimal gates & tests. Extend them as your model matures.
