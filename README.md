# Credit Analysis Bot – Refactoring Project

## What is this?

This is a personal project that marks the beginning of my coding journey.  
It was originally created when I worked as a credit analyst at a manufacturing company during the early months of the COVID-19 pandemic.

Back then, I built this bot to automate credit analysis decisions using Python, SQLite, Pandas, and Selenium — despite having no formal background in programming. The solution helped me free up time to focus on improving internal processes and business logic.

Years later, after developing my technical skills across several companies and projects, I found the original source code and decided to refactor it as a way to reflect how far I’ve come.

---

## Why refactor this?

This code represents the starting point of my transition into tech.  
The goal is to bring modern best practices into the original implementation and make the code easier to read, test, maintain, and extend.

This is not a working bot anymore (I no longer have access to the internal systems it was integrated with), but the **architecture, logic and automation flow** remain valuable and meaningful for portfolio purposes.

---

## Workplan

The initial commit contains the raw, unmodified version of the bot, with only sensitive data (e.g. URLs, credentials) removed.

The refactoring steps include:

1. Splitting code into files and folders by responsibility
2. Organizing logic into clear classes and functions
3. Adding type hints and docstrings
4. Introducing basic logging
5. Adding error handling
6. Mocking sales orders to simulate decision-making logic
7. Including unit tests to ensure code reliability

---

## Tech Stack

- Python
- Pandas
- SQLite
- Selenium
- pytest (for tests)

---

## Future Improvements

- Add Docker support for reproducibility  
- Optional CLI or API interface for interaction  
- CI/CD setup (GitHub Actions)
