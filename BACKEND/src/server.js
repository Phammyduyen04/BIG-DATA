import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import { initializeDatabase, closeDatabase } from './config/database.js';
import { config } from './config/env.js';
import marketRoutes from './routes/marketRoutes.js';
import topCoinRoutes from './routes/topCoinRoutes.js';

// Fix BigInt serialization for JSON (Prisma returns BigInt for sum, count, and BigInt columns)
BigInt.prototype.toJSON = function () {
    return this.toString();
};

const app = express();

// Middleware
app.use(helmet());
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Logging middleware
app.use((req, res, next) => {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] ${req.method} ${req.path}`);
    next();
});

// ★ Routes
app.use('/api/market', marketRoutes);
app.use('/api/top-coins', topCoinRoutes);

// Root route
app.get('/', (req, res) => {
    res.json({
        message: 'Crypto DW Backend API',
        version: '2.0.0',
        endpoints: {
            ticker24h: 'GET /api/market/:symbol_code/ticker24h',
            klines: 'GET /api/market/:symbol_code/klines?interval_code=1h&startTime=ISO&endTime=ISO',
            trades: 'GET /api/market/:symbol_code/trades?limit=50',
            symbols: 'GET /api/market/symbols',
            intervals: 'GET /api/market/intervals',
            health: 'GET /api/market/health',
            topCoins: 'GET /api/top-coins',
            topCoinDetail: 'GET /api/top-coins/:symbol_code',
        },
    });
});

// 404 handler
app.use((req, res) => {
    res.status(404).json({
        success: false,
        message: 'Route không tìm thấy',
    });
});

// Error handler
app.use((err, req, res, next) => {
    console.error('Error:', err);
    res.status(500).json({
        success: false,
        message: 'Lỗi server',
        error: config.server.env === 'development' ? err.message : undefined,
    });
});

// Khởi tạo server
const PORT = config.server.port;

async function startServer() {
    try {
        initializeDatabase();
        console.log('✓ Prisma client initialized');

        app.listen(PORT, () => {
            console.log(`✓ Server running on http://localhost:${PORT}`);
            console.log(`✓ Environment: ${config.server.env}`);
            console.log('');
            console.log('★ 3 API chính:');
            console.log(`  → GET http://localhost:${PORT}/api/market/:symbol_code/ticker24h`);
            console.log(`  → GET http://localhost:${PORT}/api/market/:symbol_code/klines?interval_code=1h&startTime=ISO&endTime=ISO`);
            console.log(`  → GET http://localhost:${PORT}/api/market/:symbol_code/trades?limit=50`);
        });
    } catch (error) {
        console.error('✗ Failed to start server:', error);
        process.exit(1);
    }
}

// Graceful shutdown
process.on('SIGTERM', async () => {
    console.log('SIGTERM received, shutting down gracefully...');
    await closeDatabase();
    process.exit(0);
});

process.on('SIGINT', async () => {
    console.log('SIGINT received, shutting down gracefully...');
    await closeDatabase();
    process.exit(0);
});

startServer();
