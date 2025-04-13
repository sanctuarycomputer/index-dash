require('dotenv').config();
const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const nodemailer = require('nodemailer');

const inputFile = 'emails.csv';
const outputFile = 'emails_with_status.csv';
const results = [];

// Google Workspace SMTP setup
const transporter = nodemailer.createTransport({
  host: 'smtp.gmail.com',
  port: 465,
  secure: true, // use SSL
  auth: {
    user: process.env.GMAIL_USER,
    pass: process.env.GMAIL_PASS,
  },
  // Increase timeout to handle potential delays
  connectionTimeout: 10000,
  greetingTimeout: 10000,
  socketTimeout: 15000,
});

// Configuration for rate limiting
const RATE_LIMIT = {
  EMAILS_PER_DAY: 2000,      // Google Workspace limit
  EMAILS_PER_MINUTE: 20,     // Conservative rate limit
  BASE_DELAY_MS: 3000,       // Base delay between emails (3 seconds)
  RETRY_BASE_DELAY_MS: 5000, // Base delay for retry (5 seconds)
  MAX_RETRIES: 3,            // Maximum retry attempts
};

// Helper: delay with exponential backoff
const delay = (ms, attemptNumber = 0) => {
  const backoffFactor = attemptNumber > 0 ? Math.pow(2, attemptNumber - 1) : 1;
  return new Promise(resolve => setTimeout(resolve, ms * backoffFactor));
};

// Helper: send email with exponential backoff retry
const sendEmailWithRetry = async (mailOptions, retries = RATE_LIMIT.MAX_RETRIES) => {
  for (let attempt = 1; attempt <= retries + 1; attempt++) {
    try {
      await transporter.sendMail(mailOptions);
      return 'SENT';
    } catch (error) {
      const errorMessage = error.message || 'Unknown error';
      console.error(`Attempt ${attempt} failed for ${mailOptions.to}:`, errorMessage);

      // Check for rate limiting or quota errors
      if (errorMessage.includes('quota') ||
          errorMessage.includes('rate limit') ||
          errorMessage.includes('450') ||
          errorMessage.includes('421') ||
          errorMessage.includes('452')) {
        console.log(`Rate limit detected, increasing delay...`);
        // Longer delay for rate limit issues
        await delay(RATE_LIMIT.RETRY_BASE_DELAY_MS * 2, attempt);
      } else if (attempt <= retries) {
        // Standard exponential backoff for other errors
        await delay(RATE_LIMIT.RETRY_BASE_DELAY_MS, attempt);
      } else {
        return `ERROR: ${errorMessage.substring(0, 50)}...`;
      }
    }
  }
  return 'FAILED';
};

// Process the emails in batches to respect rate limits
const processBatch = async (batch, startIndex) => {
  console.log(`Processing batch of ${batch.length} emails (starting at index ${startIndex})...`);

  for (let i = 0; i < batch.length; i++) {
    const row = batch[i];
    const email = row['Email Address'];
    const link = row['QV Link'];

    console.log(`Processing ${startIndex + i + 1}/${results.length}: ${email}`);

    const mailOptions = {
      from: `"Hugh from Index Space" <${process.env.GMAIL_USER}>`,
      to: email,
      cc: 'nodes@index-space.org',
      replyTo: 'nodes@index-space.org',
      subject: 'Please vote on the 2025 Index Node grant recipient!',
      text: [
        `Hi ${email},`,
        '',
        `We're reaching out because you're currently a member of one of our Nodes, we'd like you to vote on the next Index Node. Learn more about the grant program here: (https://www.index-space.org/nodes`,
        '',
        `We use a system called Quadratic Voting to give our members a weighted vote in choosing the next Index Node. More on QV here: https://www.radicalxchange.org/wiki/quadratic-voting/`,
        '',
        `Here is your personal QV link:`,
        link,
        '',
        'If you have any questions, feel free to reply to this email!',
        '',
        'Best,',
        'Index Space'
      ].join('\n')
    };

    row['Status'] = await sendEmailWithRetry(mailOptions);
    console.log(`${row['Status']}: ${email}`);

    // Delay between sends, vary slightly to avoid regular patterns
    const jitter = Math.floor(Math.random() * 1000); // Add 0-1s jitter
    await delay(RATE_LIMIT.BASE_DELAY_MS + jitter);
  }
};

// Read and process CSV
fs.createReadStream(inputFile)
  .pipe(csv())
  .on('data', (data) => results.push(data))
  .on('end', async () => {
    try {
      // Check if we're within daily limits
      if (results.length > RATE_LIMIT.EMAILS_PER_DAY) {
        console.error(`Warning: Attempting to send ${results.length} emails, which exceeds the daily limit of ${RATE_LIMIT.EMAILS_PER_DAY}.`);
        console.error('The script will continue, but some emails may not be sent due to quota limits.');
      }

      // Process in batches to respect rate limits
      const BATCH_SIZE = RATE_LIMIT.EMAILS_PER_MINUTE;

      for (let i = 0; i < results.length; i += BATCH_SIZE) {
        const batch = results.slice(i, i + BATCH_SIZE);
        await processBatch(batch, i);

        // Add a longer pause between batches if not the last batch
        if (i + BATCH_SIZE < results.length) {
          console.log(`Pausing between batches for 60 seconds to respect rate limits...`);
          await delay(60000); // 60-second pause between batches
        }
      }

      // Write results to output CSV
      const csvWriter = createCsvWriter({
        path: outputFile,
        header: [
          { id: 'Email Address', title: 'Email Address' },
          { id: 'QV Link', title: 'QV Link' },
          { id: 'Status', title: 'Status' }
        ],
      });

      await csvWriter.writeRecords(results);
      console.log(`\nAll done! Output saved to "${outputFile}".`);
    } catch (error) {
      console.error('Error during processing:', error);

      // Save progress even if there's an error
      const csvWriter = createCsvWriter({
        path: `${outputFile}.partial`,
        header: [
          { id: 'Email Address', title: 'Email Address' },
          { id: 'QV Link', title: 'QV Link' },
          { id: 'Status', title: 'Status' }
        ],
      });

      await csvWriter.writeRecords(results);
      console.log(`\nProcess failed but partial results saved to "${outputFile}.partial".`);
    }
  });