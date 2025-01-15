require("dotenv").config();
const express = require("express");
const bodyParser = require("body-parser");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const mysql = require("mysql2");

const app = express();
app.use(bodyParser.json());

const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || "your_jwt_secret";
console.log(process.env.PORT);

const DB_CONFIG = {
  host: process.env.DB_HOST || "localhost",
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASSWORD || "",
  database: process.env.DB_NAME || "jwt_auth",
};

const db = mysql.createConnection(DB_CONFIG);

db.connect((err) => {
  if (err) {
    console.error("Database connection failed:", err.stack);
    return;
  }
  console.log("Connected to MySQL database.");
});

const query = (sql, values) =>
  new Promise((resolve, reject) => {
    db.query(sql, values, (err, results) => {
      if (err) return reject(err);
      resolve(results);
    });
  });

app.post("/register", async (req, res) => {
  const { username, password, role} = req.body;
  if (!username || !password) {
    return res.status(400).json({ message: "Username and password are required" });
  }
  const userRole = role || 'user'; 
  try {
    const hashedPassword = await bcrypt.hash(password, 10);
    await query("INSERT INTO users (username, password, role) VALUES (?, ?, ?)", [
      username,
      hashedPassword,
      userRole
    ]);
    res.status(201).json({ message: "User registered successfully" });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Error registering user" });
  }
});

app.post("/login", async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) {
    return res
      .status(400)
      .json({ message: "Username and password are required" });
  }
  try {
    const users = await query("SELECT * FROM users WHERE username = ?", [
      username,
    ]);
    const user = users[0];
    if (!user) {
      return res.status(401).json({ message: "Invalid credentials" });
    }
    const isPasswordValid = await bcrypt.compare(password, user.password);
    if (!isPasswordValid) {
      return res.status(401).json({ message: "Invalid credentials" });
    }
    // Generate login metadata
    const loginTime = new Date().toISOString().replace('T', ' ').slice(0, 19);
    const loginAddress = req.ip;

    const token = jwt.sign({ id: user.id, username: user.username, role: user.role, login_time: loginTime, login_address: loginAddress }, JWT_SECRET, { expiresIn: "1h" });
    await query("INSERT INTO tokens (user_id, token, login_time, login_address) VALUES (?, ?, ?, ?)", [
        user.id,
        token,
        loginTime,
        loginAddress
      ]);
      
    res.status(200).json({ message: "Login successful", token, loginTime, loginAddress });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Error logging in" });
  }
});

app.get("/verify", async (req, res) => {
  const { token } = req.query;
  if (!token) {
    return res.status(400).json({ message: "Token is required" });
  }
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    res.status(200).json({ message: "Token is valid", decoded });
  } catch (err) {
    res.status(401).json({ message: "Invalid or expired token" });
  }
});

app.post("/logout", async (req, res) => {
  const { token } = req.body;
  if (!token) {
    return res.status(400).json({ message: "Token is required" });
  }
  try {
    await query("DELETE FROM tokens WHERE token = ?", [token]);
    res.status(200).json({ message: "Logout successful" });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Error logging out" });
  }
});

const checkAdmin = (req, res, next) => {
  const token = req.headers.authorization?.split(' ')[1]; 

  if (!token) {
    return res.status(403).json({ message: "Access denied" });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    if (decoded.role !== 'admin') {
      return res.status(403).json({ message: "Admin access required" });
    }
    req.user = decoded; 
    next(); 
  } catch (err) {
    res.status(401).json({ message: "Invalid or expired token" });
  }
};

app.get("/admin", checkAdmin, (req, res) => {
  res.status(200).json({ message: "Welcome, Admin!" });
});

// Start the server
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
