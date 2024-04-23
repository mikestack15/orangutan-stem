# Setup Guide for Dynamic Insights Scanner

Welcome to the setup guide for the Dynamic Insights Scanner project! This guide will provide you with detailed steps and educational insights to get this Django web application running on your local machine. By understanding each component and tool involved, you will gain a solid foundation in managing and developing Django applications.

## Step 1: Clone the Repository

First, we need to set up a directory to house our project files. This directory not only helps in keeping your projects organized but also separates your development environments, which is crucial when working on multiple projects.

Open your terminal or command prompt, navigate to your desired location, and execute the following commands to create a directory called `stem_projects` and navigate into it:

```
mkdir stem_projects
cd stem_projects
```

Next, clone the `orangutan-stem` repository from GitHub. This repository contains all the necessary files for the Dynamic Insights Scanner, including Django setups, configuration files, and application logic:

```
git clone https://github.com/mikestack15/orangutan-stem.git
cd orangutan-stem/projects/web_apps/dynamic_web_app
```

**Understanding Git and GitHub:**  
Git is a version control system that allows you to track changes and collaborate on projects. GitHub is a hosting service for Git repositories that provides a web-based graphical interface. It provides access control and several collaboration features, such as bug tracking, feature requests, task management, and continuous integration.

## Step 2: Create and Activate a Virtual Environment

Before starting the setup, navigate to the `barcodeproject` directory within the cloned repository:

```
cd barcodeproject
```

Create a virtual environment using Python. A virtual environment is an isolated environment that allows you to manage dependencies for the project without affecting global Python settings.

```
python -m venv venv
```

Activate the virtual environment. Activating a virtual environment will change your shell’s directory to the virtual environment, and you will use the Python and pip executable files within this environment for your project.

- On macOS/Linux:

  ```
  source venv/bin/activate
  ```

- On Windows:

  ```
  venv\Scripts\activate
  ```

## Step 3: Install Necessary Django Dependencies

Before you start working with the Django project, you need to ensure that all the necessary dependencies are installed. Django and Django REST Framework are the backbone of our application:

```
pip install Django djangorestframework
```

**Understanding Django:**  
Django is a high-level Python web framework that encourages rapid development and clean, pragmatic design. It takes care of much of the hassle of web development, so you can focus on writing your app without needing to reinvent the wheel. It’s free and open source.

**Understanding Django REST Framework:**  
Django REST Framework is a powerful toolkit for building Web APIs. It is flexible and fully featured, with capabilities for both beginners and advanced developers, providing a modular and customizable architecture that can be used to build complex web APIs.

## Step 4: Understanding Key Files and Their Functions

In your `barcodeproject` directory, you will find several files crucial for the running and configuration of your Django application. Let's discuss some of these files:

- **`urls.py`**: This file is responsible for routing URLs to the appropriate views. URL routing helps your application determine what code to execute when a request is received from a specified URL.

- **`settings.py`**: This file contains settings/configurations for this Django project like database configuration, debug options, static files settings, and application configuration. It acts as the central configuration for your Django installation.

- **`views.py`** in the `barcodeapp` directory: Views are the handlers that respond to requests from web browsers or other clients. In Django, views are Python functions or classes that receive web requests and return web responses.

## Step 5: Run the Django Development Server

To see your Django application in action, you need to start the Django development server. Run the following command from the directory containing the `manage.py` file:

```
python manage.py runserver
```

This command starts a lightweight development server on your local machine and makes your application accessible at `http://127.0.0.1:8000/` by default.

## Conclusion and Next Steps

Congratulations on setting up the Dynamic Insights Scanner project! This setup has prepared you to start developing and enhancing the application. As you progress, you will dive deeper into Django’s extensive features like models, migrations, and more advanced settings.

This guide aims to equip you with the knowledge to understand and manage a Django web application effectively. Happy coding!
