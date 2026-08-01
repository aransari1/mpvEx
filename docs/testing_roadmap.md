# Testing & Quality Assurance Roadmap — mpvRex

This document outlines the phased roadmap for integrating an automated testing, code quality, and QA gauntlet into **mpvRex**, following modern Android best practices and software engineering principles.

---

## 🎯 Goals & Objectives
1. **Prevent Regressions**: Ensure core player engine features, playlists, and subtitle parsing remain 100% reliable across app updates.
2. **Eliminate Main Thread ANRs**: Guard against concurrency deadlocks and lock contention.
3. **Automate Quality Control**: Enforce Uncle Bob's "Testing Gauntlet" (Unit Tests, Static Analysis, Coverage, Mutation Testing, BDD, and UI Benchmarks).

---

## 🛣️ Roadmap Phases: Beginner to Advanced

```
┌─────────────────────────────────────────────────────────────────────────┐
│ PHASE 1: Unit Testing Foundation (Beginner)                             │
│ • JUnit 5, MockK, Coroutines Test, Turbine                              │
│ • Unit tests for PlayerEngineManager, M3UParser, Subtitle font fallback │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ PHASE 2: Static Analysis & Code Quality Guard (Intermediate)            │
│ • Detekt, Ktlint, Android Lint                                          │
│ • Automatic main-thread I/O & coroutine deadlock detection              │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ PHASE 3: Code Coverage & Mutation Testing (Upper-Intermediate)          │
│ • Kover / JaCoCo (80% Branch Coverage Target)                           │
│ • Pitest / Stryker Mutation Testing (Kill code mutants)                 │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ PHASE 4: UI & Visual Regression Testing (Advanced)                      │
│ • Roborazzi / Paparazzi Compose Screenshot Testing                      │
│ • Layout verification across Phone, Tablet, and Foldable form factors   │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ PHASE 5: BDD & Macrobenchmark Gauntlet (Expert)                         │
│ • Gherkin Given-When-Then BDD Specifications                            │
│ • Android Macrobenchmark (Frame rendering & startup time tracking)      │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 📋 Detailed Phase Breakdown

### Phase 1: Unit Testing Foundation (Beginner)
**Focus**: Fast, deterministic JVM unit tests covering core business logic and state managers without Android emulator requirements.

* **Tools**: `junit-jupiter`, `mockk`, `kotlinx-coroutines-test`, `app.cash.turbine`
* **Target Components**:
  1. `PlayerEngineManagerTest`:
     - Test `tryLock(500ms)` lock acquisition under simulated thread contention.
     - Verify engine state transitions (`IDLE` → `FOREGROUND_PLAYING` → `TEARDOWN`).
     - Verify `awaitTeardownSync()` properly awaits in-flight teardown jobs before re-initialization.
  2. `M3UParserTest`:
     - Verify parsing of standard and extended M3U/M3U8 playlists (EXTINF tags, headers, stream URLs).
     - Test corrupt/empty playlist fallback handling.
  3. `SubtitleFontFallbackTest`:
     - Test font selection logic: when `sub-font` preference is blank, verify `"subfont, Roboto, sans-serif"` is emitted to prevent vendor Variable Font space-rendering bugs.
* **Standard Command**:
  ```bash
  ./gradlew test
  ```

---

### Phase 2: Static Analysis & Code Quality Guard (Intermediate)
**Focus**: Automated static analysis tools running in CI/build pipeline to enforce formatting and prevent code smells.

* **Tools**: `Detekt`, `Ktlint`, `Android Lint`
* **Rules & Constraints**:
  - **Detekt**: Enforce cyclomatic complexity limits (< 15 per function), detect unhandled exceptions, catch unsafe blocking calls in coroutine scopes.
  - **Android Lint**: Flag main-thread disk I/O, unclosed streams (`InputStream`/`OutputStream`), and deprecated API usages.
  - **Ktlint**: Enforce zero style deviations across all Kotlin source files.
* **Standard Command**:
  ```bash
  ./gradlew lintDebug detekt
  ```

---

### Phase 3: Code Coverage & Mutation Testing (Upper-Intermediate)
**Focus**: Verifying that unit test coverage is thorough and assertions actually validate business contracts.

* **Tools**: `Kover` (Kotlin Code Coverage), `Pitest` / `Stryker`
* **Deliverables**:
  - Set a minimum threshold of **80% branch coverage** for `xyz.mpv.rex.ui.player.engine` and `xyz.mpv.rex.utils`.
  - Run mutation testing: inject deliberate code mutants (e.g., inverting conditional branches or modifying mathematical operations) and ensure the unit test suite kills 100% of mutants.
* **Standard Command**:
  ```bash
  ./gradlew koverHtmlReport
  ```

---

### Phase 4: UI & Visual Regression Testing (Advanced)
**Focus**: Ensuring Jetpack Compose UI components render flawlessly without visual regressions across screen sizes.

* **Tools**: `Roborazzi` / `Paparazzi`
* **Target Components**:
  - `PlayerControls`: Top/bottom bar rendering, seek bar, time labels.
  - `SpeedControlSlider`: Custom slider formatting and speed presets.
  - `SubtitlesPreferencesScreen`: Typography cards, dropdown menus, color pickers.
* **Validation**: Generate baseline PNG snapshots and run automated pixel-diff checks on pull requests.

---

### Phase 5: BDD & Macrobenchmark Gauntlet (Expert)
**Focus**: User-centric behavior specifications and real-device performance benchmarking.

* **Tools**: `Kotest BDD` / `Cucumber`, `androidx.benchmark:benchmark-macro`
* **Deliverables**:
  1. **Behavior Specs (Gherkin)**:
     ```gherkin
     Feature: Background Playback Transition
       Scenario: App minimized during active playback
         Given video playback is active
         When user leaves the app via home gesture
         Then background service starts with active notification
     ```
  2. **Performance Benchmarking**:
     - Cold Startup Time: Measure time-to-first-frame (< 100ms goal).
     - Frame Pacing: Ensure UI thread frame rendering budget stays strictly under 8.3ms (for 120Hz displays).

---

## 🛠️ Standard Execution Commands

| Task | Standard Command |
| :--- | :--- |
| **Run Unit Tests** | `./gradlew test` |
| **Run Static Analysis** | `./gradlew lintDebug detekt` |
| **Generate Coverage Report** | `./gradlew koverHtmlReport` |
| **Full Verification** | `./gradlew check` |

> [!NOTE]
> In local Termux / AndroidIDE environments, developers may append `-I local-env.gradle.kts` to inject local environment optimizations if needed.
