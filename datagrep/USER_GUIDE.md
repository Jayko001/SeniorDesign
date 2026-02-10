# Datagrep User Guide & Manual

Welcome to the Datagrep User Guide! This manual provides step-by-step instructions for installing, configuring, and using the Datagrep platform, including both the web interface and Slack bot integration. It is designed for users of all technical backgrounds.

---

## Table of Contents
1. [Introduction](#introduction)
2. [Installation & Setup](#installation--setup)
3. [Quickstart Guide](#quickstart-guide)
4. [Using the Web Interface](#using-the-web-interface)
5. [Using the Slack Bot](#using-the-slack-bot)
6. [Troubleshooting](#troubleshooting)
7. [FAQ](#faq)
8. [Additional Resources](#additional-resources)

---

## Introduction
Datagrep is a platform for data exploration, pipeline generation, and visualization. It offers a web interface and a Slack bot for easy interaction with your data.

---

## Installation & Setup

### Prerequisites
- [Docker](https://www.docker.com/get-started) installed
- [Git](https://git-scm.com/downloads) installed
- API keys for required services (see below)

### Steps
1. **Clone the repository:**
   ```sh
   git clone https://github.com/your-org/datagrep.git
   cd datagrep
   ```
2. **Set up environment variables:**
   - Copy `env.example` to `.env` in the backend directory and fill in your API keys.
3. **Start the services:**
   ```sh
   docker-compose up --build
   ```
4. **Access the web app:**
   - Open your browser and go to [http://localhost:3000](http://localhost:3000)

---

## Quickstart Guide
- After starting the services, visit the web interface.
- Log in or sign up if required.
- Upload your data or connect to a data source.
- Use the dashboard to explore, query, and visualize your data.

---

## Using the Web Interface

1. **Home Page:**
   - Overview of features and recent activity.
2. **Data Upload:**
   - Click 'Upload' to add new datasets.
3. **Query Builder:**
   - Use the query builder to create and run data queries.
4. **Visualization:**
   - Generate charts and graphs from your data.

*Screenshot placeholders:*
- ![Home Page](images/web_home_placeholder.png)
- ![Upload Data](images/upload_placeholder.png)
- ![Visualization](images/visualization_placeholder.png)

---

## Using the Slack Bot

1. **Add the Slack bot to your workspace.**
2. **Configure environment variables as described in `SLACK_BOT_README.md`.**
3. **Interact with the bot:**
   - Use `/datagrep` commands in Slack to query data, generate pipelines, or request visualizations.

*Screenshot placeholder:*
- ![Slack Bot Example](images/slack_bot_placeholder.png)

---

## Troubleshooting
- **Docker won’t start:** Ensure Docker Desktop is running.
- **Web app not loading:** Check that all containers are up (`docker ps`).
- **Slack bot not responding:** Verify environment variables and Slack permissions.
- For more, see [SETUP.md](SETUP.md) and [SLACK_BOT_README.md](SLACK_BOT_README.md).

---

## FAQ

**Q: Do I need coding experience to use Datagrep?**
A: No, the web interface is designed for all users. Advanced features may require some data knowledge.

**Q: How do I reset my password?**
A: Use the 'Forgot Password' link on the login page.

**Q: Can I use my own data?**
A: Yes, upload your datasets via the web interface.

**Q: What if I get an error about missing API keys?**
A: Double-check your `.env` file in the backend directory.

**Q: How do I update Datagrep?**
A: Pull the latest code and restart the containers.

---

## Additional Resources
- [SETUP.md](SETUP.md): Advanced setup and troubleshooting
- [SLACK_BOT_README.md](SLACK_BOT_README.md): Slack bot integration
- [User Stories](../HW_assignments/Shared/User_Stories.md)
- [Design Diagrams](../Design_Diagrams/)

---

*For further help, contact the development team or open an issue on GitHub.*
