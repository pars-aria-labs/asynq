# Contributing

Thanks for your interest in contributing to Asynq!
We are open to, and grateful for, any contributions made by the community.

## Reporting Bugs

Have a look at our [issue tracker](https://github.com/pars-aria-labs/asynq/issues). If you can't find an issue (open or closed)
describing your problem (or a very similar one) there, please open a new issue with
the following details:

- Which versions of Go and Redis are you using?
- What are you trying to accomplish?
- What is the full error you are seeing?
- How can we reproduce this?
  - Please quote as much of your code as needed to reproduce (best link to a
    public repository or Gist)

## Getting Help

The [go-asynq Gitter channel](https://gitter.im/go-asynq/community) belongs to
the upstream project. This fork does not operate that channel and cannot
guarantee that fork-specific questions will be answered there.

For help with behavior or APIs provided by this fork, first search the
[Pars Aria Labs issue tracker](https://github.com/pars-aria-labs/asynq/issues).
If your question has not already been answered, open a focused issue with a
small example and the versions of Go, Redis, and Asynq you are using. Do not
include credentials or sensitive incident details in a public issue.

## Submitting Feature Requests

If you can't find an issue (open or closed) describing your idea on our [issue
tracker](https://github.com/pars-aria-labs/asynq/issues), open an issue. Adding answers to the following
questions in your description is +1:

- What do you want to do, and how do you expect Asynq to support you with that?
- How might this be added to Asynq?
- What are possible alternatives?
- Are there any disadvantages?

Thank you! We'll try to respond as quickly as possible.

## Contributing Code

1. Fork this repo
2. Download your fork `git clone git@github.com:your-username/asynq.git && cd asynq`
3. Create your branch `git checkout -b your-branch-name`
4. Make and commit your changes
5. Push the branch `git push origin your-branch-name`
6. Create a new pull request

Please keep your pull request focused and avoid unrelated changes. Run the
standalone test suite locally and, when Docker is available, exercise the Redis
Cluster path with `--redis_cluster`. CI repeats the suite against both a
standalone Redis service and a three-node cluster.

After you have submitted your pull request, we'll try to get back to you as soon as possible. We may suggest some changes or improvements.

Thank you for contributing!
