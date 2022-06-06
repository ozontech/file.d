# Welcome to file.d contributing guide
Thank you for investing your time in contributing to our project! This set of basic guidelines will help us make file.d development as easy, effective and transparent as possible for everyone involved.
In this guide you will get an overview of the contribution workflow from opening an issue, creating a PR, reviewing, and merging the PR.

## Issues
### Bug reports
Before reporting a bug please use the Github search and check if the issue has already been reported/fixed.

A great bug report should include: 
- A quick summary and/or background
- Steps to reproduce
- Sample code (if necessary)
- What you expected would happen
- What actually happens
- Notes (possibly including why you think this might be happening, or stuff you tried that didn't work)
- Context -- platform, versions of the software software and everything that you think may be important to reproduce and fix the newfound bug.

Here's an example of a [great bug report](http://www.openradar.me/11905408).
Make sure to use the bug report template available when creating a new issue.


### Feature requests
Please provide as many details and as much context as possible. Use the feature request template available when reporting a new issue.

## Pull requests
Every pull request description should include the number of the issue it's resolving. Usually this looks like "Closes #<>" text in the PR description.

Pull requests with new features, bug fixes and improvements are very helpful and should remain focused on one main thing and avoid containing unrelated commits.
Please stick to the coding conventions used throughout the project (indentation, accurate comments, etc.).

Adhering to the following process is the best way to get your work included in the project:
1. Fork the project, clone your fork, and configure the remotes.
```bash
git clone https://github.com/<your-username>/file.d.git
cd file.d
git remote add upstream git@github.com:ozontech/file.d.git
```
2. If you cloned a while ago, get the latest changes from upstream.
```bash
git checkout master
git pull upstream master
```
3. Create a new topic branch (off the master branch) to contain your feature, change, or fix.
  The branch name should have the following format <issue-no>-<short-topic-name>: the number of the issue that this branch solves, followed by a hyphen and a short name for the main feature you're developing.
  Awesome branch names: `80-contrib-guide`, `76-ban-metric`.
  Branch names that should be avoided: `no-gh-issue-number`, `80`, `very-long-feature-description-bla-bla-bla-bla`.
4. Commit your changes in logical chunks. Please adhere to these [git commit message guidelines](https://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html) or your code is unlikely be merged into the main project. Use Git's [interactive rebase](https://docs.github.com/en/get-started/using-git/about-git-rebase) feature to tidy up your commits before making them public.
5. Locally merge the upstream master into your topic branch
```bash
git pull [--rebase] upstream master
```
6. Push the branch to your fork
```bash
git push origin <topic-branch-name>
```
7. Open a pull request with a clear title and description.

## Code style

* log in lower case without a dot in the end of a sentence;
* log params by pattern: `name=value`, e.g. `logger.Errorf("can't parse url=%s", url)`;
* code comments should be written with a small letter without a dot at the end;
* split imports into 2 groups: stdlib and other packages; 

## Collaborating guidelines

The main rule for now is that every PR requires at least two approvals from the core file.d team members, unless it's a small fix (like a typo). Usually by approve we mean commment "LGTM" in the PR.

Note that this set of rules can (and should) be extended in the future.  
Feel free to discuss and propose improvements to this guidelines!

