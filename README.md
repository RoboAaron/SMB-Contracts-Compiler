# SMB Contracts Compiler

A comprehensive government contract opportunity aggregator focused on South Texas, designed specifically for small and medium businesses.

## 🎯 **Project Overview**

SMB Contracts Compiler automatically scrapes and aggregates contract opportunities from multiple government procurement portals in the South Texas region, providing a unified interface for businesses to discover and track relevant opportunities.

### **Target Region**
- **Houston** (Harris County)
- **San Antonio** (Bexar County) 
- **Corpus Christi** (Nueces County)
- **Brownsville** (Cameron County)
- **McAllen** (Hidalgo County)
- **Victoria** (Victoria County)
- **State of Texas** (ESBD portal)

### **Key Features**
- **Daily Automated Scraping**: Pulls latest opportunities from all target portals
- **Duplicate Detection**: Prevents duplicate opportunities across portals
- **Unified Search Interface**: Single dashboard to browse all opportunities
- **Advanced Filtering**: Filter by location, value, due date, and more
- **Export Capabilities**: Export to CSV/Excel for further analysis
- **Real-time Updates**: Live status indicators and last scrape timestamps

## 🚀 **Quick Start**

### Prerequisites
- Python 3.8+
- PostgreSQL 12+
- Chrome/Chromium (for Selenium scraping)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/SMB-Contracts-Compiler.git
   cd SMB-Contracts-Compiler
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Setup database**
   ```bash
   # Create PostgreSQL database
   createdb smb_contracts_compiler
   
   # Run migrations
   alembic upgrade head
   ```

5. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

6. **Run the application**
   ```bash
   python -m src.web.main
   ```

## 📁 **Project Structure**

```
SMB-Contracts-Compiler/
├── src/
│   ├── scrapers/          # Web scraping modules
│   ├── database/          # Database models and repositories
│   ├── web/              # Web application (FastAPI)
│   └── utils/            # Utility functions
├── tests/                # Test suite
├── config/               # Configuration files
├── docs/                 # Documentation
└── alembic/              # Database migrations
```

## 🔧 **Configuration**

The application uses YAML configuration files in the `config/` directory:

- `default.yaml`: Default configuration
- `scraper_config.yaml`: Portal-specific scraping settings

## 🧪 **Testing**

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src

# Run specific test categories
pytest tests/unit/
pytest tests/integration/
```

## 📊 **API Documentation**

Once the application is running, visit:
- **Dashboard**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

## 🤝 **Contributing**

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 **License**

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 **Support**

For support and questions:
- Create an issue on GitHub
- Check the [documentation](docs/)
- Review the [troubleshooting guide](docs/troubleshooting.md)
