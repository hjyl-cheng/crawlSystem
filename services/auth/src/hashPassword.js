import bcrypt from "bcryptjs";

const chunks = [];
for await (const chunk of process.stdin) chunks.push(chunk);
const password = Buffer.concat(chunks).toString("utf8").replace(/[\r\n]+$/, "");
if (password.length < 16) {
  console.error("password must contain at least 16 characters");
  process.exit(2);
}
process.stdout.write(`${await bcrypt.hash(password, 12)}\n`);
