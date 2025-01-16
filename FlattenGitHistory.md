Yes, it is possible to squash all commits in a Git repository into a single commit, effectively rewriting the repository's history. Here's how to do it:

### Steps to Squash All Commits

1. **Ensure Your Branch is Clean**:
    - Make sure there are no uncommitted changes in your working directory. Run:
      ```bash
      git status
      ```
    - If there are changes, commit or stash them.

2. **Find the Initial Commit**:
    - Locate the first commit in the repository:
      ```bash
      git rev-list --max-parents=0 HEAD
      ```
    - This will output the hash of the initial commit.

3. **Start an Interactive Rebase**:
    - Begin an interactive rebase for the entire branch's history:
      ```bash
      git rebase -i --root
      ```

4. **Mark All Commits for Squashing**:
    - A text editor will open, showing all commits in the repository. The first commit will likely say `pick` and subsequent commits may say `pick` as well.
    - Change all `pick` entries (except the first one) to `squash` (or `s` for short).

5. **Edit the Commit Message**:
    - After marking the commits for squashing, Git will prompt you to edit the commit message for the single commit that will result from the squash.
    - You can combine all commit messages or write a new one.

6. **Complete the Rebase**:
    - Save and close the editor to complete the rebase. Git will squash all commits into one.

7. **Force Push the New History**:
    - If this branch is already shared with others (e.g., on a remote), you need to force push to replace the history:
      ```bash
      git push --force
      ```

---

### Example

Suppose the commit history looks like this:

```
commit abc123: Add README
commit def456: Initial implementation
commit ghi789: Fix bugs
```

After squashing all commits, it will become:

```
commit xyz000: Initial commit (or a new combined message)
```

---

### Important Notes
- **Force Push Implications**: If this branch has been shared with others, force-pushing can cause conflicts for collaborators.
- **Backup Your Repository**: Consider creating a backup branch before rewriting history:
  ```bash
  git branch backup
  ```
- **Why Use Squashing**: This is typically used for cleaning up a branch before merging or for simplifying a repository's history.

If you're comfortable with rewriting history, this approach will leave you with a single, clean commit.