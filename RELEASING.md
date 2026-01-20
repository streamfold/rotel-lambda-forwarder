# Release Process

This document describes the automated release process for rotel-lambda-forwarder.

## Overview

The release process has been streamlined with automation to reduce manual steps. You now have two options for creating a release:

1. **GitHub Actions (Recommended)** - Fully automated via web UI
2. **Manual Process** - Traditional manual steps

## Option 1: GitHub Actions (Recommended)

**NOTE**: This automation requires a Personal Access Token, defined in the secret `PAT_RELEASE_ENGINEER`, with the following permissions:
- Contents: read/write
- Metadata: read
- Pull requests: read/write

### Step 1: Trigger Version Bump Workflow

1. Go to the [Actions tab](../../actions/workflows/bump-version.yml) in GitHub
2. Click on "Bump Version".
3. Click "Run workflow"
4. Select the version bump type:
   - `patch` - Bug fixes (0.0.4 → 0.0.5)
   - `minor` - New features (0.0.4 → 0.1.0)
   - `major` - Breaking changes (0.0.4 → 1.0.0)
5. Click "Run workflow"

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

### Step 3: Automatic Tag and Release Creation

When the version bump PR is merged:
- ✅ A tag `v<version>` is automatically created and pushed
- ✅ The [auto-release workflow](../../actions/workflows/auto-release.yml) is automatically triggered
- ✅ A GitHub release is created with auto-generated release notes
- ✅ The [release workflow](../../actions/workflows/release.yml) is then triggered by the release creation
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

### Step 7: Monitor Release Workflows

When a tag is pushed, two workflows run sequentially:

1. The [auto-release workflow](../../actions/workflows/auto-release.yml) will:
   - Validate the tag format (must be vX.Y.Z)
   - Check if a release already exists
   - Generate release notes
   - Create the GitHub release

2. The [release workflow](../../actions/workflows/release.yml) will then:
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
