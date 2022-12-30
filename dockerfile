FROM python:3.8

# Clone the repository
RUN git clone https://github.com/rajsinghtech/pptracker.git

# Change to the repository directory
WORKDIR "/pptracker"

# Copy the .env file to the repository directory
COPY .env .

# Install the required libraries
RUN pip3 install -r requirements.txt

# Run the main script
CMD ["python", "main.py"]