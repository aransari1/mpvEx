package xyz.mpv.rex.preferences

import xyz.mpv.rex.preferences.preference.PreferenceStore

/**
 * Preferences for folder management
 */
class FoldersPreferences(
  preferenceStore: PreferenceStore,
) {
  // Set of folder paths that should be hidden from the folder list
  val blacklistedFolders = preferenceStore.getStringSet("blacklisted_folders", emptySet())

  // Persisted SAF roots explicitly authorized for hybrid library indexing.
  val libraryScanRoots = preferenceStore.getStringSet("library_scan_roots", emptySet())
}
