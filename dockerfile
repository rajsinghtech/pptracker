FROM python:3.8
WORKDIR "/pptracker"
# Clone the repository
COPY main.py .

# Copy the .env file to the repository directory
COPY .env .
COPY requirements.txt .

# Install the required libraries
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

# Run the main script

CMD ["python", "main.py"]