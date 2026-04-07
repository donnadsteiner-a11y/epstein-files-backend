const pool = require('../db/pool');

const PLAN_DEFINITIONS = Object.freeze({
  tier_1: Object.freeze({
    label: 'Tier 1',
    saved_search_limit: 5,
    report_limit: 5,
    can_save_searches: true,
    can_create_reports: true,
    can_download_reports: true,
    can_print_reports: true,
    can_use_bookmarks: false,
    can_view_history: false,
  }),
  tier_2: Object.freeze({
    label: 'Tier 2',
    saved_search_limit: 5,
    report_limit: 20,
    can_save_searches: true,
    can_create_reports: true,
    can_download_reports: true,
    can_print_reports: true,
    can_use_bookmarks: true,
    can_view_history: true,
  }),
  tier_3: Object.freeze({
    label: 'Tier 3',
    saved_search_limit: null,
    report_limit: null,
    can_save_searches: true,
    can_create_reports: true,
    can_download_reports: true,
    can_print_reports: true,
    can_use_bookmarks: true,
    can_view_history: true,
  }),
});

function normalizePlanTier(planTier) {
  return PLAN_DEFINITIONS[planTier] ? planTier : 'tier_1';
}

function isUnlimited(limit) {
  return limit === null;
}

function getRemaining(limit, used) {
  if (isUnlimited(limit)) return null;
  return Math.max(limit - used, 0);
}

function getPlanEntitlements(planTier) {
  const normalized = normalizePlanTier(planTier);
  const definition = PLAN_DEFINITIONS[normalized];

  return {
    plan_tier: normalized,
    ...definition,
  };
}

function buildUsageSnapshot(entitlements, counts = {}) {
  const savedSearchesUsed = Number(counts.saved_searches || 0);
  const reportsUsed = Number(counts.reports || 0);

  return {
    saved_searches: {
      used: savedSearchesUsed,
      limit: entitlements.saved_search_limit,
      remaining: getRemaining(entitlements.saved_search_limit, savedSearchesUsed),
      unlimited: isUnlimited(entitlements.saved_search_limit),
    },
    reports: {
      used: reportsUsed,
      limit: entitlements.report_limit,
      remaining: getRemaining(entitlements.report_limit, reportsUsed),
      unlimited: isUnlimited(entitlements.report_limit),
    },
  };
}

async function getUserAccessProfile(userId) {
  const result = await pool.query(
    `SELECT id, plan_tier
       FROM users
      WHERE id = $1 AND is_active = TRUE`,
    [userId]
  );

  if (!result.rows.length) {
    return null;
  }

  const user = result.rows[0];
  return {
    id: user.id,
    plan_tier: normalizePlanTier(user.plan_tier),
    entitlements: getPlanEntitlements(user.plan_tier),
  };
}

module.exports = {
  buildUsageSnapshot,
  getPlanEntitlements,
  getRemaining,
  getUserAccessProfile,
  isUnlimited,
  normalizePlanTier,
  PLAN_DEFINITIONS,
};
