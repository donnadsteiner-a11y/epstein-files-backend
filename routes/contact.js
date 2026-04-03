/**
 * Contact route
 *   POST /api/contact   — submit contact form, email to support@docketzero.com
 *
 * Accepts JSON only (no file attachments).
 * Saves submission to DB and sends email notification.
 */

const express    = require('express');
const nodemailer = require('nodemailer');
const rateLimit  = require('express-rate-limit');
const pool       = require('../db/pool');

const router = express.Router();

// Rate limit — 5 submissions per hour per IP
const contactLimit = rateLimit({
  windowMs: 60 * 60 * 1000,
  max: 5,
  message: { error: 'Too many contact submissions. Please try again in an hour.' },
});

// ── Email transporter ──────────────────────────────────────────────────────────
function createTransporter() {
  return nodemailer.createTransport({
    host:   process.env.SMTP_HOST,
    port:   parseInt(process.env.SMTP_PORT || '587'),
    secure: process.env.SMTP_PORT === '465',
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS,
    },
    tls: {
      rejectUnauthorized: process.env.NODE_ENV === 'production',
    },
  });
}

// ── POST /api/contact ──────────────────────────────────────────────────────────
router.post('/', contactLimit, async (req, res) => {
  try {
    const { first_name, last_name, email, role, subject, message } = req.body;

    // Validate required fields
    if (!first_name?.trim()) return res.status(400).json({ error: 'First name is required.' });
    if (!last_name?.trim())  return res.status(400).json({ error: 'Last name is required.' });
    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.trim())) {
      return res.status(400).json({ error: 'A valid email address is required.' });
    }
    if (!subject?.trim()) return res.status(400).json({ error: 'Subject is required.' });
    if (!message?.trim()) return res.status(400).json({ error: 'Message is required.' });

    const ipAddress = req.headers['x-forwarded-for']?.split(',')[0] || req.ip;

    // Save to database
    await pool.query(
      `INSERT INTO contact_submissions
         (first_name, last_name, email, role, subject, message, file_count, ip_address)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [
        first_name.trim(),
        last_name.trim(),
        email.trim().toLowerCase(),
        role || null,
        subject.trim(),
        message.trim(),
        0,
        ipAddress,
      ]
    );

    // Subject labels
    const subjectLabels = {
      general:    'General Inquiry',
      media:      'Media / Press Inquiry',
      research:   'Research Collaboration',
      legal:      'Legal / Takedown Request',
      technical:  'Technical Issue',
      other:      'Other',
    };

    const subjectLabel = subjectLabels[subject] || subject;

    // Build HTML email
    const htmlBody = `
      <div style="font-family:Arial,sans-serif;max-width:600px;color:#0f1720">
        <div style="background:#102131;padding:20px 28px;border-radius:8px 8px 0 0">
          <h2 style="margin:0;color:#fff;font-size:18px">New DocketZero Contact Submission</h2>
          <p style="margin:6px 0 0;color:#91afc8;font-size:14px">
            ${new Date().toLocaleString('en-US', { timeZone: 'America/New_York' })} ET
          </p>
        </div>
        <div style="background:#f9fbfd;padding:24px 28px;border:1px solid #dbe4ec;border-top:none">
          <table style="width:100%;border-collapse:collapse;font-size:14px">
            <tr>
              <td style="padding:8px 0;color:#4a6070;width:140px"><strong>Name</strong></td>
              <td style="padding:8px 0">${first_name.trim()} ${last_name.trim()}</td>
            </tr>
            <tr>
              <td style="padding:8px 0;color:#4a6070"><strong>Email</strong></td>
              <td style="padding:8px 0"><a href="mailto:${email.trim()}">${email.trim()}</a></td>
            </tr>
            <tr>
              <td style="padding:8px 0;color:#4a6070"><strong>Role</strong></td>
              <td style="padding:8px 0">${role || 'Not specified'}</td>
            </tr>
            <tr>
              <td style="padding:8px 0;color:#4a6070"><strong>Subject</strong></td>
              <td style="padding:8px 0">${subjectLabel}</td>
            </tr>
          </table>
          <hr style="border:none;border-top:1px solid #dbe4ec;margin:16px 0">
          <p style="margin:0 0 8px;color:#4a6070;font-size:13px"><strong>Message</strong></p>
          <div style="background:#fff;border:1px solid #dbe4ec;border-radius:8px;padding:16px;
                      font-size:14px;line-height:1.7;white-space:pre-wrap">${message.trim()}</div>
        </div>
        <div style="background:#eef3f8;padding:12px 28px;border-radius:0 0 8px 8px;
                    border:1px solid #dbe4ec;border-top:none">
          <p style="margin:0;font-size:12px;color:#4a6070">
            Reply directly to this email to respond to ${first_name.trim()}.
          </p>
        </div>
      </div>
    `;

    const transporter = createTransporter();

    // Email 1 — notification to DocketZero
    await transporter.sendMail({
      from:    `"DocketZero Contact" <${process.env.SMTP_USER}>`,
      to:      process.env.CONTACT_TO_EMAIL || 'support@docketzero.com',
      replyTo: email.trim(),
      subject: `[DocketZero] ${subjectLabel} — ${first_name.trim()} ${last_name.trim()}`,
      html:    htmlBody,
    });

    // Email 2 — confirmation to the sender
    const confirmHtml = `
      <div style="font-family:Arial,sans-serif;max-width:600px;color:#0f1720">
        <div style="background:#102131;padding:20px 28px;border-radius:8px 8px 0 0">
          <img src="https://docketzero.com/assets/docketzero_logo_horizontal.png"
               alt="DocketZero" style="height:36px;margin-bottom:4px" />
        </div>
        <div style="background:#f9fbfd;padding:28px;border:1px solid #dbe4ec;border-top:none">
          <h2 style="margin:0 0 12px;font-size:20px;color:#0f1720">
            Thank you for reaching out, ${first_name.trim()}.
          </h2>
          <p style="margin:0 0 16px;font-size:15px;line-height:1.7;color:#304355">
            We've received your message and will get back to you as soon as possible —
            typically within 48 hours.
          </p>
          <div style="background:#fff;border:1px solid #dbe4ec;border-radius:8px;
                      padding:16px 20px;margin-bottom:20px">
            <p style="margin:0 0 6px;font-size:12px;font-weight:700;color:#7a96ae;
                      text-transform:uppercase;letter-spacing:.05em">Your message</p>
            <p style="margin:0;font-size:14px;line-height:1.7;color:#304355;
                      white-space:pre-wrap">${message.trim()}</p>
          </div>
          <p style="margin:0;font-size:14px;color:#304355;line-height:1.7">
            DocketZero is an independent preservation archive of publicly released government
            records. If you have a time-sensitive matter, you can reach us directly at
            <a href="mailto:support@docketzero.com" style="color:#193146;font-weight:700">
              support@docketzero.com</a>.
          </p>
        </div>
        <div style="background:#eef3f8;padding:14px 28px;border-radius:0 0 8px 8px;
                    border:1px solid #dbe4ec;border-top:none">
          <p style="margin:0;font-size:12px;color:#7a96ae">
            DocketZero · Preserving the public record ·
            <a href="https://docketzero.com" style="color:#7a96ae">docketzero.com</a>
          </p>
        </div>
      </div>
    `;

    await transporter.sendMail({
      from:    `"DocketZero" <${process.env.SMTP_USER}>`,
      to:      email.trim(),
      subject: `We received your message — DocketZero`,
      html:    confirmHtml,
    });

    console.log(`[CONTACT] Submission from ${email.trim()} — ${subjectLabel}`);

    res.json({ success: true, message: 'Message received. We will be in touch soon.' });

  } catch (err) {
    console.error('[CONTACT] Error:', err.message);
    res.status(500).json({
      error: 'Could not send message. Please try again or email support@docketzero.com directly.',
    });
  }
});

module.exports = router;
