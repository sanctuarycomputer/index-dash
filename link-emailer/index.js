require('dotenv').config();
const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const nodemailer = require('nodemailer');

const inputFile = 'emails.csv';
const outputFile = 'emails_with_status.csv';
const results = [];

// Gmail SMTP setup
const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.GMAIL_USER,
    pass: process.env.GMAIL_PASS,
  },
});

// Helper: delay between sends
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Helper: send email with retry
const sendEmailWithRetry = async (mailOptions, retries = 2) => {
  for (let attempt = 1; attempt <= retries + 1; attempt++) {
    try {
      await transporter.sendMail(mailOptions);
      return 'SENT';
    } catch (error) {
      console.error(`Attempt ${attempt} failed for ${mailOptions.to}:`, error.message);
      if (attempt <= retries) {
        await delay(1000); // 1 second before retry
      } else {
        return 'ERROR';
      }
    }
  }
};

// Read and process CSV
fs.createReadStream(inputFile)
  .pipe(csv())
  .on('data', (data) => results.push(data))
  .on('end', async () => {
    for (const row of results) {
      const email = row['Email Address'];
      const link = row['QV Link'];

      const mailOptions = {
        from: `"Index Space" <${process.env.GMAIL_USER}>`,
        to: email,
        cc: 'nodes@index-space.org',
        replyTo: 'nodes@index-space.org',
        subject: 'Here is your personalized link!',
        text: [
          `Hi ${email},`,
          '',
          `We're reaching out because you're currently a member of one of our Nodes, we'd like you to vote on the next Index Node. Learn more about the grant program here: https://www.index-space.org/nodes`,
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

      await delay(500); // Delay between sends
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
  });