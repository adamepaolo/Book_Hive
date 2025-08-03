import sqlite3
from flask import Flask, render_template, request, redirect, url_for, session, flash
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
import os
from functools import wraps # IMPORTANT: Ensure this import is present for decorators
import logging # Import logging module

# Configure basic logging for the Flask app
logging.basicConfig(level=logging.DEBUG) # Changed to DEBUG for more verbose logging during debugging
app = Flask(__name__)
app.config['SECRET_KEY'] = 'your_super_secret_key_here'  # Replace with a strong secret key
app.config['DATABASE'] = 'book_hive.db'

# --- Decorators ---
def login_required(f):
    """Decorates routes to require login."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.', 'error')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    """Decorates routes to require admin privileges."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('is_admin'):
            flash('Unauthorized access. Admins only.', 'error')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

def librarian_or_admin_required(f):
    """Decorates routes to require librarian or admin privileges."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not (session.get('is_admin') or session.get('is_librarian')):
            flash('Unauthorized access. Admins and Librarians only.', 'error')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# --- Database Functions ---
def get_db_connection():
    """Establishes and returns a database connection."""
    conn = sqlite3.connect(app.config['DATABASE'])
    conn.row_factory = sqlite3.Row  # This allows accessing columns by name
    return conn

def init_db():
    """Initializes the database schema and populates with default data if empty."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Create users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            first_name TEXT,
            last_name TEXT,
            is_admin BOOLEAN DEFAULT 0,
            is_librarian BOOLEAN DEFAULT 0,
            is_approved BOOLEAN DEFAULT 0
        )
    ''')
    # Create books table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            author TEXT,
            category TEXT,
            publisher TEXT,
            price REAL NOT NULL,
            book_condition TEXT NOT NULL DEFAULT 'New',
            book_status TEXT NOT NULL DEFAULT 'Available',
            is_available BOOLEAN DEFAULT 1
        )
    ''')
    # Create orders table (for purchases)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            order_date TEXT NOT NULL,
            total_amount REAL NOT NULL,
            status TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    ''')
    # Create order_items table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            book_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            price_at_purchase REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(id),
            FOREIGN KEY (book_id) REFERENCES books(id)
        )
    ''')
    # Create payments table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            payment_date TEXT NOT NULL,
            amount REAL NOT NULL,
            payment_type TEXT,
            status TEXT,
            FOREIGN KEY (order_id) REFERENCES orders(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    ''')
    # Create borrowed_books table for tracking borrowed items
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS borrowed_books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            book_id INTEGER NOT NULL,
            borrow_date TEXT NOT NULL,
            return_date TEXT,
            status TEXT NOT NULL DEFAULT 'Borrowed',
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (book_id) REFERENCES books(id)
        )
    ''')
    # Create new table for borrow requests
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS borrow_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            book_id INTEGER NOT NULL,
            request_date TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'Pending',
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (book_id) REFERENCES books(id)
        )
    ''')
    conn.commit()

    # Add new columns to existing tables if they don't exist (migration helper)
    def add_column_if_not_exists(table_name, column_name, column_type, default_value):
        try:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type} DEFAULT {default_value}")
            conn.commit()
            app.logger.info(f"Added '{column_name}' column to '{table_name}' table.")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                app.logger.info(f"'{column_name}' column already exists in '{table_name}' table.")
            else:
                raise e

    add_column_if_not_exists('users', 'is_approved', 'BOOLEAN', 0)
    add_column_if_not_exists('users', 'is_librarian', 'BOOLEAN', 0)
    add_column_if_not_exists('books', 'book_condition', 'TEXT', "'New'")
    add_column_if_not_exists('books', 'book_status', 'TEXT', "'Available'")
    add_column_if_not_exists('books', 'is_available', 'BOOLEAN', 1)

    # Add default admin user if not exists
    cursor.execute("SELECT * FROM users WHERE username = 'admin'")
    if not cursor.fetchone():
        hashed_password = generate_password_hash('adminpass')
        cursor.execute(
            "INSERT INTO users (username, email, password, first_name, last_name, is_admin, is_librarian, is_approved) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ('admin', 'admin@bookhive.com', hashed_password, 'Admin', 'User', 1, 0, 1)
        )
        conn.commit()
        app.logger.info("Default admin user created: username='admin', password='adminpass'")

    # Add default librarian user if not exists
    cursor.execute("SELECT * FROM users WHERE username = 'librarian'")
    if not cursor.fetchone():
        hashed_password = generate_password_hash('libpass')
        cursor.execute(
            "INSERT INTO users (username, email, password, first_name, last_name, is_admin, is_librarian, is_approved) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ('librarian', 'librarian@bookhive.com', hashed_password, 'Library', 'Keeper', 0, 1, 1)
        )
        conn.commit()
        app.logger.info("Default librarian user created: username='librarian', password='libpass'")

    # Add sample books if the table is empty
    cursor.execute("SELECT COUNT(*) FROM books")
    if cursor.fetchone()[0] == 0:
        selling_books = [
            ('The Quantum Realm', 'Dr. Alice Smith', 'Science Fiction', 'Future Press', 25.99, 'New', 'Available', 1),
            ('Culinary Delights', 'Chef Antoine', 'Cookbook', 'Gourmet Prints', 32.50, 'New', 'Available', 1),
            ('Secrets of the Ancient City', 'Prof. Indiana Jones', 'History', 'Discovery Books', 18.00, 'Second Hand', 'Available', 1),
            ('Digital Marketing Mastery', 'Sarah SEO', 'Business', 'Innovate Publishing', 45.00, 'New', 'Available', 1),
            ('Art of Minimalist Living', 'Zen Master', 'Self-Help', 'Harmony House', 15.75, 'New', 'Available', 1),
            ('Galactic Explorers', 'Captain Kirk', 'Space Opera', 'Starbound Books', 29.99, 'New', 'Available', 1),
            ('The Silent Witness', 'Agatha Christie', 'Mystery', 'Classic Reads', 10.50, 'Second Hand', 'Available', 1),
            ('Coding for Beginners', 'Dev Guru', 'Technology', 'Code Publishers', 22.00, 'New', 'Available', 1),
            ('Gardening for Dummies', 'Green Thumb', 'Hobby', 'Outdoor Living', 14.99, 'New', 'Available', 1),
            ('Financial Freedom', 'Mr. Moneybags', 'Finance', 'Wealth Creators', 39.99, 'New', 'Available', 1),
            ('The Lost Artifact', 'Archaeologist Ann', 'Adventure', 'Ancient Lore', 21.00, 'New', 'Available', 1),
            ('Healthy Eating Guide', 'Nutritionist Nora', 'Health', 'Wellness Books', 17.50, 'New', 'Available', 1),
            ('Travel the World on a Budget', 'Wanderlust Will', 'Travel', 'Global Guides', 13.00, 'Second Hand', 'Available', 1),
            ('Understanding AI', 'Dr. Robot', 'Technology', 'Future Minds', 55.00, 'New', 'Available', 1),
            ('The Art of Photography', 'Lens Master', 'Art', 'Visual Arts Press', 28.00, 'New', 'Available', 1),
            ('Mythical Creatures Compendium', 'Lorelei Legend', 'Fantasy', 'Enchanted Scrolls', 20.00, 'New', 'Available', 1),
            ('Space Colonization', 'Elon Musk', 'Science', 'Mars Books', 49.99, 'New', 'Available', 1),
            ('Effective Communication', 'Speaker Sam', 'Self-Help', 'Voice Publishing', 16.25, 'New', 'Available', 1),
            ('The History of Jazz', 'Melody Maker', 'Music', 'Rhythm Books', 24.00, 'New', 'Available', 1),
            ('Quantum Computing Explained', 'Dr. Qubit', 'Technology', 'Bitstream Press', 60.00, 'New', 'Available', 1)
        ]
        borrowing_books = [
            ('Introduction to Python', 'Guido van Rossum', 'Programming', 'Open Source Pub', 0.00, 'New', 'Available', 1),
            ('Classic Fairy Tales', 'Various Authors', 'Children', 'Storytime Press', 0.00, 'Second Hand', 'Available', 1),
            ('World Atlas 2024', 'Cartography Dept.', 'Reference', 'Map Makers Inc.', 0.00, 'New', 'Available', 1),
            ('Basic Algebra', 'Math Whiz', 'Education', 'Equation Books', 0.00, 'New', 'Available', 1),
            ('The Art of Public Speaking', 'Orator Owen', 'Self-Help', 'Voice Masters', 0.00, 'Second Hand', 'Available', 1),
            ('Beginner\'s Guide to Chess', 'Grandmaster G.', 'Hobby', 'Strategy Games', 0.00, 'New', 'Available', 1),
            ('Introduction to Philosophy', 'Socrates Jr.', 'Philosophy', 'Thinkers Press', 0.00, 'New', 'Available', 1),
            ('Cooking for One', 'Solo Chef', 'Cookbook', 'Single Serve Pub', 0.00, 'Second Hand', 'Available', 1),
            ('Yoga for Stress Relief', 'Calm Cathy', 'Health', 'Mind Body Books', 0.00, 'New', 'Available', 1),
            ('Short Stories for Long Nights', 'Anthology', 'Fiction', 'Dream Weaver', 0.00, 'New', 'Available', 1),
            ('DIY Home Repairs', 'Handy Harry', 'Hobby', 'Fix It Yourself', 0.00, 'Second Hand', 'Available', 1),
            ('The Wonders of Nature', 'Naturalist Nick', 'Science', 'Green Earth Books', 0.00, 'New', 'Available', 1),
            ('Learn Spanish in 30 Days', 'Lingua Lingo', 'Language', 'Polyglot Press', 0.00, 'New', 'Available', 1),
            ('Introduction to Economics', 'Adam Smithy', 'Economics', 'Market Insights', 0.00, 'Second Hand', 'Available', 1),
            ('A Brief History of Time', 'Stephen Hawking', 'Science', 'Cosmos Books', 0.00, 'New', 'Available', 1)
        ]
        cursor.executemany(
            "INSERT INTO books (title, author, category, publisher, price, book_condition, book_status, is_available) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            selling_books + borrowing_books
        )
        conn.commit()
        app.logger.info("Sample books added to the database.")
    else:
        app.logger.info("Books already exist in the database. Skipping sample data insertion.")

    conn.close()
    app.logger.info("Database initialized.") # Confirmation message for init_db


# Initialize database on app startup
with app.app_context():
    init_db()


# Context processor to make datetime available in all templates
@app.context_processor
def inject_now():
    return {'datetime': datetime}


# Before request: check if user is logged in and populate session with user info
@app.before_request
def before_request():
    if 'user_id' in session:
        conn = get_db_connection()
        user = conn.execute("SELECT * FROM users WHERE id = ?", (session['user_id'],)).fetchone()
        conn.close()
        if user:
            # Convert Row object to dictionary for easier access in templates
            session['user'] = dict(user)
            session['username'] = user['username'] # Ensure username is consistently set
            session['is_admin'] = bool(user['is_admin'])
            session['is_librarian'] = bool(user['is_librarian'])
        else:
            # User not found (e.g., deleted), clear session
            session.pop('user_id', None)
            session.pop('user', None)
            session.pop('username', None)
            session.pop('is_admin', None)
            session.pop('is_librarian', None)
    else:
        # Clear session variables if user is not logged in
        session.pop('user', None)
        session.pop('username', None)
        session.pop('is_admin', None)
        session.pop('is_librarian', None)


# --- Routes ---
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        email = request.form['email']
        password = request.form['password']
        confirm_password = request.form['confirm_password']
        first_name = request.form.get('first_name')
        last_name = request.form.get('last_name')

        if not username or not email or not password or not confirm_password:
            flash('All fields are required!', 'error')
            return render_template('register.html')

        if password != confirm_password:
            flash('Passwords do not match!', 'error')
            return render_template('register.html')

        hashed_password = generate_password_hash(password)

        conn = get_db_connection()
        try:
            conn.execute(
                "INSERT INTO users (username, email, password, first_name, last_name, is_admin, is_librarian, is_approved) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (username, email, hashed_password, first_name, last_name, 0, 0, 0)
            )
            conn.commit()
            flash('Registration successful! Your account is awaiting administrator approval before you can log in.', 'success')
            return redirect(url_for('login'))
        except sqlite3.IntegrityError:
            flash('Username or Email already exists.', 'error')
        finally:
            conn.close()
    return render_template('register.html')


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        identifier = request.form['identifier']
        password = request.form['password']

        conn = get_db_connection()
        user = conn.execute(
            "SELECT * FROM users WHERE username = ? OR email = ?",
            (identifier, identifier)
        ).fetchone()
        conn.close()

        if user:
            if not bool(user['is_approved']):
                flash('Your account is awaiting administrator approval.', 'warning')
                return render_template('login.html')

            if check_password_hash(user['password'], password):
                session['user_id'] = user['id']
                session['username'] = user['username']
                session['is_admin'] = bool(user['is_admin'])
                session['is_librarian'] = bool(user['is_librarian'])
                flash('Logged in successfully!', 'success')
                return redirect(url_for('index'))
            else:
                flash('Invalid username/email or password.', 'error')
        else:
            flash('Invalid username/email or password.', 'error')
    return render_template('login.html')


@app.route('/logout')
@login_required
def logout():
    session.pop('user_id', None)
    session.pop('username', None)
    session.pop('is_admin', None)
    session.pop('is_librarian', None)
    session.pop('user', None)
    flash('You have been logged out.', 'info')
    return redirect(url_for('index'))


# Display books available for borrowing (price = 0)
@app.route('/books')
def books():
    conn = get_db_connection()
    # Only fetch books with price = 0 for the 'borrow' page
    books_list = conn.execute("SELECT * FROM books WHERE price = 0 AND book_status IN ('Available', 'On Shelves')").fetchall()
    conn.close()
    return render_template('books.html', books=books_list)

# Display books available for sale (price > 0)
@app.route('/books_for_sale')
def books_for_sale():
    conn = get_db_connection()
    # Only fetch books with price > 0 that are available
    books_list = conn.execute(
        "SELECT * FROM books WHERE price > 0 AND book_status IN ('Available', 'On Shelves')").fetchall()
    conn.close()
    return render_template('order_books.html', books=books_list)


@app.route('/manage_books')
@librarian_or_admin_required
def manage_books():
    conn = get_db_connection()
    books_list = conn.execute("SELECT * FROM books").fetchall()
    conn.close()
    return render_template('manage_books.html', books=books_list)


@app.route('/add_book', methods=['GET', 'POST'])
@librarian_or_admin_required
def add_book():
    if request.method == 'POST':
        title = request.form['title']
        author = request.form['author']
        category = request.form['category']
        publisher = request.form['publisher']
        price = request.form['price']
        book_condition = request.form['book_condition']
        book_status = request.form['book_status']
        is_available = 1 if book_status in ['Available', 'On Shelves'] else 0

        if not all([title, author, category, publisher, price, book_condition, book_status]):
            flash('All fields are required!', 'error')
            return render_template('add_book.html')

        try:
            price = float(price)
            if price < 0:
                flash('Price cannot be negative.', 'error')
                return render_template('add_book.html')
        except ValueError:
            flash('Invalid price. Must be a number.', 'error')
            return render_template('add_book.html')

        conn = get_db_connection()
        try:
            conn.execute(
                "INSERT INTO books (title, author, category, publisher, price, book_condition, book_status, is_available) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (title, author, category, publisher, price, book_condition, book_status, is_available)
            )
            conn.commit()
            flash(f'Book "{title}" added successfully!', 'success')
            return redirect(url_for('manage_books'))
        except Exception as e:
            flash(f'Error adding book: {e}', 'error')
            app.logger.error(f"Error adding book: {e}") # Log the error
        finally:
            conn.close()
    return render_template('add_book.html')


@app.route('/edit_book/<int:book_id>', methods=['GET', 'POST'])
@librarian_or_admin_required
def edit_book(book_id):
    conn = get_db_connection()
    book = conn.execute("SELECT * FROM books WHERE id = ?", (book_id,)).fetchone()

    if not book:
        flash('Book not found.', 'error')
        conn.close()
        return redirect(url_for('manage_books'))

    if request.method == 'POST':
        title = request.form['title']
        author = request.form['author']
        category = request.form['category']
        publisher = request.form['publisher']
        price = request.form['price']
        book_condition = request.form['book_condition']
        book_status = request.form['book_status']
        is_available = 1 if book_status in ['Available', 'On Shelves'] else 0

        if not all([title, author, category, publisher, price, book_condition, book_status]):
            flash('All fields are required!', 'error')
            conn.close()
            return render_template('edit_book.html', book=book)

        try:
            price = float(price)
            if price < 0:
                flash('Price cannot be negative.', 'error')
                conn.close()
                return render_template('edit_book.html', book=book)
        except ValueError:
            flash('Invalid price. Must be a number.', 'error')
            conn.close()
            return render_template('edit_book.html', book=book)

        try:
            conn.execute(
                "UPDATE books SET title = ?, author = ?, category = ?, publisher = ?, price = ?, book_condition = ?, book_status = ?, is_available = ? WHERE id = ?",
                (title, author, category, publisher, price, book_condition, book_status, is_available, book_id)
            )
            conn.commit()
            flash(f'Book "{title}" updated successfully!', 'success')
            return redirect(url_for('manage_books'))
        except Exception as e:
            flash(f'Error updating book: {e}', 'error')
            app.logger.error(f"Error updating book {book_id}: {e}") # Log the error
        finally:
            conn.close()

    conn.close()
    return render_template('edit_book.html', book=book)


@app.route('/delete_book/<int:book_id>', methods=['POST'])
@librarian_or_admin_required
def delete_book(book_id):
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM borrowed_books WHERE book_id = ?", (book_id,))
        conn.execute("DELETE FROM borrow_requests WHERE book_id = ?", (book_id,))
        conn.execute("DELETE FROM order_items WHERE book_id = ?", (book_id,))
        conn.execute("DELETE FROM books WHERE id = ?", (book_id,))
        conn.commit()
        flash('Book deleted successfully!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting book: {e}', 'error')
        app.logger.error(f"Error deleting book {book_id}: {e}") # Log the error
    finally:
        conn.close()
    return redirect(url_for('manage_books'))


@app.route('/borrow/<int:book_id>')
@login_required
def borrow_book(book_id):
    conn = get_db_connection()
    book = conn.execute("SELECT * FROM books WHERE id = ? AND price = 0 AND book_status IN ('Available', 'On Shelves')",
                        (book_id,)).fetchone()

    if not book:
        flash('Book not found or not available for borrowing (it might be for sale).', 'error')
        conn.close()
        return redirect(url_for('books'))

    try:
        user_id = session['user_id']
        request_date = datetime.now().isoformat()

        existing_request = conn.execute(
            """
            SELECT * FROM borrow_requests
            WHERE user_id = ? AND book_id = ? AND status = 'Pending'
            """,
            (user_id, book_id)
        ).fetchone()

        existing_borrow = conn.execute(
            """
            SELECT * FROM borrowed_books
            WHERE user_id = ? AND book_id = ? AND status = 'Borrowed'
            """,
            (user_id, book_id)
        ).fetchone()

        if existing_request:
            flash(f'You already have a pending borrow request for "{book["title"]}".', 'info')
            conn.close()
            return redirect(url_for('dashboard'))

        if existing_borrow:
            flash(f'You have already borrowed "{book["title"]}" and have not returned it yet.', 'info')
            conn.close()
            return redirect(url_for('dashboard'))

        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO borrow_requests (user_id, book_id, request_date, status) VALUES (?, ?, ?, ?)",
            (user_id, book_id, request_date, 'Pending')
        )
        conn.commit()
        flash(f'Borrow request for "{book["title"]}" submitted successfully! Awaiting librarian approval.', 'success')
        return redirect(url_for('dashboard'))
    except Exception as e:
        conn.rollback()
        flash(f'Error submitting borrow request: {e}', 'error')
        app.logger.error(f"Error submitting borrow request for user {user_id}, book {book_id}: {e}") # Log the error
        return redirect(url_for('books'))
    finally:
        if conn: # Ensure conn is not None before closing
            conn.close()


@app.route('/purchase/<int:book_id>', methods=['POST'])
@login_required
def purchase_book(book_id):
    user_id = session['user_id']
    conn = get_db_connection()
    book = conn.execute("SELECT * FROM books WHERE id = ? AND price > 0 AND book_status IN ('Available', 'On Shelves')",
                        (book_id,)).fetchone()

    if not book:
        flash('Book not found or not available for purchase.', 'error')
        conn.close()
        return redirect(url_for('books_for_sale'))

    try:
        order_date = datetime.now().isoformat()
        total_amount = book['price']
        status = 'Completed'

        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO orders (user_id, order_date, total_amount, status) VALUES (?, ?, ?, ?)",
            (user_id, order_date, total_amount, status)
        )
        order_id = cursor.lastrowid

        cursor.execute(
            "INSERT INTO order_items (order_id, book_id, quantity, price_at_purchase) VALUES (?, ?, ?, ?)",
            (order_id, book['id'], 1, book['price'])
        )

        conn.execute(
            "UPDATE books SET book_status = ?, is_available = 0 WHERE id = ?",
            ('Sold', book_id)
        )
        conn.commit()
        flash(f'Successfully purchased "{book["title"]}" for ${book["price"]:.2f}!', 'success')
        return redirect(url_for('dashboard'))
    except Exception as e:
        conn.rollback()
        flash(f'Error during purchase: {e}', 'error')
        app.logger.error(f"Error during purchase for user {user_id}, book {book_id}: {e}") # Log the error
        return redirect(url_for('books_for_sale'))
    finally:
        if conn: # Ensure conn is not None before closing
            conn.close()


@app.route('/return/<int:borrow_id>')
@login_required
def return_book(borrow_id):
    conn = get_db_connection()
    borrow_record = conn.execute(
        "SELECT * FROM borrowed_books WHERE id = ? AND user_id = ? AND status = 'Borrowed'",
        (borrow_id, session['user_id'])
    ).fetchone()

    if not borrow_record:
        flash('Borrowed record not found or already returned.', 'error')
        conn.close()
        return redirect(url_for('dashboard'))

    try:
        book_id = borrow_record['book_id']
        return_date = datetime.now().isoformat()

        conn.execute(
            "UPDATE borrowed_books SET status = ?, return_date = ? WHERE id = ?",
            ('Returned', return_date, borrow_id)
        )

        conn.execute(
            "UPDATE books SET book_status = ?, is_available = 1 WHERE id = ?",
            ('On Shelves', book_id)
        )
        conn.commit()
        flash(f'Successfully returned book!', 'success')
        return redirect(url_for('dashboard'))
    except Exception as e:
        conn.rollback()
        flash(f'Error during return process: {e}', 'error')
        app.logger.error(f"Error returning book for borrow_id {borrow_id}: {e}") # Log the error
        return redirect(url_for('dashboard'))
    finally:
        if conn: # Ensure conn is not None before closing
            conn.close()


@app.route('/donate_book', methods=['GET', 'POST'])
@login_required
def donate_book():
    if request.method == 'POST':
        title = request.form['title']
        author = request.form['author']
        category = request.form['category']
        publisher = request.form['publisher']
        book_condition = 'Second Hand'
        book_status = 'On Shelves'
        price = 0.0

        if not all([title, author, category, publisher]):
            flash('All fields are required for donation!', 'error')
            return render_template('donate_book.html')

        conn = get_db_connection()
        try:
            conn.execute(
                "INSERT INTO books (title, author, category, publisher, price, book_condition, book_status, is_available) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (title, author, category, publisher, price, book_condition, book_status, 1)
            )
            conn.commit()
            flash(f'Thank you for donating "{title}"!', 'success')
            return redirect(url_for('books'))
        except Exception as e:
            flash(f'Error processing donation: {e}', 'error')
            app.logger.error(f"Error processing donation for book {title}: {e}") # Log the error
        finally:
            if conn: # Ensure conn is not None before closing
                conn.close()
    return render_template('donate_book.html')

@app.route('/dashboard')
@login_required
def dashboard():
    user_id = session['user_id']
    conn = None
    user_orders_list = []
    user_borrowed_books = []
    user_borrow_requests = []

    # Debugging marker: This line confirms this specific dashboard function is being run.
    app.logger.debug("Dashboard route accessed - Confirmed version.")

    try:
        conn = get_db_connection()
        app.logger.debug("Dashboard: Database connection established.")

        # Fetch all relevant order and book data for purchases
        raw_orders_data = conn.execute(
            """
            SELECT o.id AS order_id, o.order_date, o.total_amount, o.status,
                   b.title AS book_title, b.author, oi.quantity, oi.price_at_purchase
            FROM orders o
            JOIN order_items oi ON o.id = oi.order_id
            JOIN books b ON oi.book_id = b.id
            WHERE o.user_id = ?
            ORDER BY o.order_date DESC
            """,
            (user_id,)
        ).fetchall()
        app.logger.debug(f"Dashboard: Fetched {len(raw_orders_data)} raw order data rows.")


        # Group order items by order ID for easier display
        user_orders_dict = {}
        for row in raw_orders_data:
            order_id = row['order_id']
            if order_id not in user_orders_dict:
                user_orders_dict[order_id] = {
                    'order_id': row['order_id'],
                    'order_date': row['order_date'],
                    'total_amount': row['total_amount'],
                    'status': row['status'],
                    'items': []
                }
            user_orders_dict[order_id]['items'].append({
                'book_title': row['book_title'],
                'author': row['author'],
                'quantity': row['quantity'],
                'price_at_purchase': row['price_at_purchase']
            })
        user_orders_list = list(user_orders_dict.values())
        app.logger.debug(f"Dashboard: Processed {len(user_orders_list)} user orders.")


        # Fetch borrowed books for the user
        user_borrowed_books = conn.execute(
            """
            SELECT bb.id AS borrow_id, bb.borrow_date, bb.return_date, bb.status AS borrow_status,
                   b.title AS book_title, b.author, b.book_condition
            FROM borrowed_books bb
            JOIN books b ON bb.book_id = b.id
            WHERE bb.user_id = ?
            ORDER BY bb.borrow_date DESC
            """,
            (user_id,)
        ).fetchall()
        app.logger.debug(f"Dashboard: Fetched {len(user_borrowed_books)} borrowed books.")


        # Fetch pending borrow requests for the user
        user_borrow_requests = conn.execute(
            """
            SELECT br.id AS request_id, br.request_date, br.status AS request_status,
                   b.title AS book_title, b.author
            FROM borrow_requests br
            JOIN users u ON br.user_id = u.id
            JOIN books b ON br.book_id = b.id
            WHERE br.user_id = ? AND br.status = 'Pending'
            ORDER BY br.request_date DESC
            """,
            (user_id,)
        ).fetchall()
        app.logger.debug(f"Dashboard: Fetched {len(user_borrow_requests)} pending borrow requests.")


        return render_template('dashboard.html',
                               user_orders=user_orders_list,
                               borrowed_books=user_borrowed_books,
                               borrow_requests=user_borrow_requests)

    except Exception as e:
        app.logger.error(f"Error retrieving dashboard data for user {user_id}: {e}")
        flash(f'An error occurred while retrieving your dashboard data. Please try again later. (Error: {str(e)})', 'error')
        return redirect(url_for('index'))

    finally:
        if conn:
            conn.close()


@app.route('/manage_users')
@admin_required
def manage_users():
    conn = get_db_connection()
    users_list = conn.execute("SELECT * FROM users ORDER BY is_approved ASC, username ASC").fetchall()
    conn.close()
    return render_template('manage_users.html', users=users_list)


@app.route('/approve_user/<int:user_id>', methods=['POST'])
@admin_required
def approve_user(user_id):
    conn = get_db_connection()
    try:
        conn.execute("UPDATE users SET is_approved = 1 WHERE id = ?", (user_id,))
        conn.commit()
        flash('User approved successfully!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error approving user: {e}', 'error')
        app.logger.error(f"Error approving user {user_id}: {e}") # Log the error
    finally:
        if conn: # Ensure conn is not None before closing
            conn.close()
    return redirect(url_for('manage_users'))


@app.route('/delete_user/<int:user_id>', methods=['POST'])
@admin_required
def delete_user(user_id):
    conn = get_db_connection()
    try:
        if user_id == session.get('user_id'):
            flash('You cannot delete your own admin account.', 'error')
            return redirect(url_for('manage_users'))

        target_user = conn.execute("SELECT is_admin, is_librarian FROM users WHERE id = ?", (user_id,)).fetchone()
        if target_user and (bool(target_user['is_admin']) or bool(target_user['is_librarian'])):
            flash('Cannot delete an administrator or librarian account.', 'error')
            return redirect(url_for('manage_users'))

        conn.execute("DELETE FROM borrowed_books WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM payments WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM borrow_requests WHERE user_id = ?", (user_id,))

        order_ids_to_delete = conn.execute("SELECT id FROM orders WHERE user_id = ?", (user_id,)).fetchall()
        for order_id_row in order_ids_to_delete:
            conn.execute("DELETE FROM order_items WHERE order_id = ?", (order_id_row['id'],))

        conn.execute("DELETE FROM orders WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.commit()
        flash('User and associated data deleted successfully!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting user: {e}', 'error')
        app.logger.error(f"Error deleting user {user_id}: {e}") # Log the error
    finally:
        if conn: # Ensure conn is not None before closing
            conn.close()
    return redirect(url_for('manage_users'))


@app.route('/manage_borrow_requests')
@librarian_or_admin_required
def manage_borrow_requests():
    conn = get_db_connection()
    pending_requests = conn.execute(
        """
        SELECT br.id AS request_id, br.request_date,
               u.username, u.email,
               b.title AS book_title, b.author
        FROM borrow_requests br
        JOIN users u ON br.user_id = u.id
        JOIN books b ON br.book_id = b.id
        WHERE br.status = 'Pending'
        ORDER BY br.request_date ASC
        """
    ).fetchall()
    conn.close()
    return render_template('manage_borrow_requests.html', pending_requests=pending_requests)


@app.route('/approve_borrow_request/<int:request_id>', methods=['POST'])
@librarian_or_admin_required
def approve_borrow_request(request_id):
    conn = get_db_connection()
    request_record = conn.execute(
        "SELECT * FROM borrow_requests WHERE id = ? AND status = 'Pending'",
        (request_id,)
    ).fetchone()

    if not request_record:
        flash('Borrow request not found or already processed.', 'error')
        conn.close()
        return redirect(url_for('manage_borrow_requests'))

    try:
        user_id = request_record['user_id']
        book_id = request_record['book_id']
        borrow_date = datetime.now().isoformat()

        book_info = conn.execute(
            "SELECT title, book_status FROM books WHERE id = ?", (book_id,)
        ).fetchone()

        if not book_info or book_info['book_status'] not in ['Available', 'On Shelves']:
            flash(f'Book "{book_info["title"]}" is no longer available.', 'error')
            conn.execute("UPDATE borrow_requests SET status = 'Rejected' WHERE id = ?", (request_id,))
            conn.commit()
            conn.close()
            return redirect(url_for('manage_borrow_requests'))

        conn.execute(
            "UPDATE books SET book_status = ?, is_available = 0 WHERE id = ?",
            ('Borrowed', book_id)
        )

        conn.execute(
            "INSERT INTO borrowed_books (user_id, book_id, borrow_date, status) VALUES (?, ?, ?, ?)",
            (user_id, book_id, borrow_date, 'Borrowed')
        )

        conn.execute(
            "UPDATE borrow_requests SET status = 'Approved' WHERE id = ?",
            (request_id,)
        )
        conn.commit()
        flash(f'Borrow request for "{book_info["title"]}" approved successfully!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error approving borrow request: {e}', 'error')
        app.logger.error(f"Error approving borrow request {request_id}: {e}") # Log the error
    finally:
        if conn: # Ensure conn is not None before closing
            conn.close()
    return redirect(url_for('manage_borrow_requests'))


@app.route('/reject_borrow_request/<int:request_id>', methods=['POST'])
@librarian_or_admin_required
def reject_borrow_request(request_id):
    conn = get_db_connection()
    request_record = conn.execute(
        "SELECT * FROM borrow_requests WHERE id = ? AND status = 'Pending'",
        (request_id,)
    ).fetchone()

    if not request_record:
        flash('Borrow request not found or already processed.', 'error')
        conn.close()
        return redirect(url_for('manage_borrow_requests'))

    try:
        conn.execute(
            "UPDATE borrow_requests SET status = 'Rejected' WHERE id = ?",
            (request_id,)
        )
        conn.commit()
        flash('Borrow request rejected.', 'info')
    except Exception as e:
        conn.rollback()
        flash(f'Error rejecting borrow request: {e}', 'error')
        app.logger.error(f"Error rejecting borrow request {request_id}: {e}") # Log the error
    finally:
        if conn: # Ensure conn is not None before closing
            conn.close()
    return redirect(url_for('manage_borrow_requests'))


if __name__ == '__main__':
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)
    # Ensure logging is configured before running the app
    import logging
    logging.basicConfig(level=logging.INFO) # Keep INFO for general runtime, DEBUG for specific debugging
    app.run(debug=True)
