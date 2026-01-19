# Release Process

This document describes the automated release process for rotel-lambda-forwarder.

## Overview

The release process has been streamlined with automation to reduce manual steps. You now have two options for creating a release:

1. **GitHub Actions (Recommended)** - Fully automated via web UI
2. **Manual Process** - Traditional manual steps

## Option 1: GitHub Actions (Recommended)

### Step 1: Trigger Version Bump Workflow

1. Go to the [Actions tab](../../actions/workflows/bump-version.yml) in GitHub
2. Click "Run workflow"
3. Select the version bump type:
   - `patch` - Bug fixes (0.0.4 → 0.0.5)
   - `minor` - New features (0.0.4 → 0.1.0)
   - `major` - Breaking changes (0.0.4 → 1.0.0)
4. Click "Run workflow"

This will:
- ✅ Read the current version from `Cargo.toml`
- ✅ Calculate the new version
- ✅ Create a branch `re/bump-for-<version>`
- ✅ Update `Cargo.toml` and `Cargo.lock`
- ✅ Commit and push the changes
- ✅ Open a pull request automatically

### Step 2: Review and Merge PR

1. Review the automatically created PR
2. Approve and merge to `main`

### Step 3: Automatic Tag Creation

When the version bump PR is merged:
- ✅ A tag `v<version>` is automatically created and pushed
- ✅ The [release workflow](../../actions/workflows/release.yml) is automatically triggered
- ✅ Binaries are built for both x86_64 and arm64
- ✅ Assets are uploaded to S3 and GitHub releases
- ✅ CloudFormation templates are generated and uploaded

### Step 4: Edit Release Notes (Optional)

1. Go to the [Releases page](../../releases)
2. Find the newly created release
3. Edit the auto-generated release notes if needed
4. Save changes

### Preventing Automatic Tagging

If you need to merge a version bump PR without creating a release:

1. Add the `no-auto-tag` label to the PR before merging
2. The automatic tagging will be skipped
3. You can manually create the tag later:
   ```bash
   git tag v<version>
   git push origin v<version>
   ```

## Option 2: Manual Process

If you prefer to do everything manually:

### Step 1: Determine Next Version

```bash
# Read current version
grep '^version = ' Cargo.toml | head -1

# Calculate next version (bump patch by 1)
# Example: 0.0.4 → 0.0.5
```

### Step 2: Create Branch

```bash
git checkout main
git pull
git checkout -b re/bump-for-<version>
```

### Step 3: Update Files

1. Edit `Cargo.toml` and update the `version` field
2. Run `cargo build` to update `Cargo.lock`

### Step 4: Commit and Push

```bash
git add Cargo.toml Cargo.lock
git commit -m "Bump version to <version>"
git push origin re/bump-for-<version>
```

### Step 5: Create and Merge PR

1. Open a pull request on GitHub
2. Review and approve
3. Merge to `main`

### Step 6: Create Tag

The tag will be created automatically unless you add the `no-auto-tag` label to the PR.

To manually create the tag:

```bash
git checkout main
git pull
git tag v<version>
git push origin v<version>
```

### Step 7: Monitor Release Workflow

The [release workflow](../../actions/workflows/release.yml) will automatically:
- Build binaries for x86_64 and arm64
- Upload to S3 and GitHub releases
- Generate and upload CloudFormation templates

## Release Artifacts

Each release produces the following artifacts:

### GitHub Release Assets

- `rotel-lambda-forwarder-v<version>_x86_64.zip` - x86_64 Lambda function
- `rotel-lambda-forwarder-v<version>_arm64.zip` - ARM64 Lambda function

### S3 Buckets

**Production Bucket:** `rotel-lambda-forwarder`

Versioned artifacts:
- `s3://rotel-lambda-forwarder/rotel-lambda-forwarder/v<version>/x86_64/rotel-lambda-forwarder.zip`
- `s3://rotel-lambda-forwarder/rotel-lambda-forwarder/v<version>/arm64/rotel-lambda-forwarder.zip`

Latest artifacts:
- `s3://rotel-lambda-forwarder/rotel-lambda-forwarder/latest/x86_64/rotel-lambda-forwarder.zip`
- `s3://rotel-lambda-forwarder/rotel-lambda-forwarder/latest/arm64/rotel-lambda-forwarder.zip`

### CloudFormation Templates

**Production Bucket:** `rotel-cloudformation`

Versioned templates:
- `s3://rotel-cloudformation/stacks/v<version>/x86_64/rotel-lambda-forwarder-otlp.yaml`
- `s3://rotel-cloudformation/stacks/v<version>/x86_64/rotel-lambda-forwarder-clickhouse.yaml`
- `s3://rotel-cloudformation/stacks/v<version>/arm64/rotel-lambda-forwarder-otlp.yaml`
- `s3://rotel-cloudformation/stacks/v<version>/arm64/rotel-lambda-forwarder-clickhouse.yaml`

Latest templates:
- `s3://rotel-cloudformation/stacks/latest/x86_64/rotel-lambda-forwarder-otlp.yaml`
- `s3://rotel-cloudformation/stacks/latest/x86_64/rotel-lambda-forwarder-clickhouse.yaml`
- `s3://rotel-cloudformation/stacks/latest/arm64/rotel-lambda-forwarder-otlp.yaml`
- `s3://rotel-cloudformation/stacks/latest/arm64/rotel-lambda-forwarder-clickhouse.yaml`

## Troubleshooting

### GitHub Actions Workflow Fails

1. Check the workflow logs in the Actions tab
2. Common issues:
   - Version conflict (version already exists)
   - Permissions issues
   - Build failures

### Tag Already Exists

The auto-tag workflow will **fail** if a tag already exists. This is a safety feature to prevent accidentally overwriting releases.

**Error message in workflow:**
```
❌ ERROR: Tag vX.Y.Z already exists!
This usually means:
  1. A release for this version was already created
  2. The version in Cargo.toml was not bumped correctly
```

**Common causes:**
1. You merged a version bump PR twice without bumping the version
2. Someone manually created the tag already
3. The version in Cargo.toml wasn't actually incremented

**To fix:**

If the tag is incorrect and should be deleted:
```bash
# Delete local tag
git tag -d v<version>

# Delete remote tag
git push --delete origin v<version>
```

If the version bump was incorrect:
1. Create a new version bump PR with the correct (higher) version
2. Merge the new PR
3. The auto-tag workflow will succeed with the new version

### PR Merged But Tag Not Created

Check if:
1. The PR had the `no-auto-tag` label
2. The PR branch matched the pattern `re/bump-for-*`
3. The auto-tag workflow ran successfully

To manually create the tag:
```bash
git checkout main
git pull
git tag v<version>
git push origin v<version>
```

### Build Failures

If the release build fails:
1. Check the [release workflow logs](../../actions/workflows/release.yml)
2. Fix any build issues
3. Delete the tag and recreate it to re-trigger the workflow

## Semantic Versioning Guidelines

This project follows [Semantic Versioning](https://semver.org/):

- **MAJOR** version (X.0.0): Breaking changes, incompatible API changes
- **MINOR** version (0.X.0): New features, backwards compatible
- **PATCH** version (0.0.X): Bug fixes, backwards compatible

### When to Bump What

- **Patch**: Bug fixes, documentation updates, internal refactoring
- **Minor**: New features, new configuration options, dependency updates
- **Major**: Breaking changes, API changes, removal of features

## Pre-Release Testing

Before creating a release, ensure:

1. ✅ All CI tests pass on `main`
2. ✅ Manual testing completed
3. ✅ Documentation updated
4. ✅ CHANGELOG updated (if maintained)

The [pre-release workflow](../../actions/workflows/pre-release.yml) automatically builds and uploads artifacts to the dev S3 bucket on every push to `main`, allowing you to test before creating an official release.

## Questions?

If you have questions or encounter issues with the release process, please:

1. Check this documentation
2. Review the workflow files in `.github/workflows/`
3. Check existing issues or create a new one