#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

// Load .env file from project root (before any env validation)
try {
    require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') });
} catch (e) {
    // dotenv not installed; will be caught by validation below
}

console.log('🚀 Starting deployment preparations...');

// Run Prisma migrations for database setup
async function runPrismaMigrations() {
    const timestamp = new Date().toISOString();
    console.log(`🗃️ [${timestamp}] Running Prisma migrations...`);
    console.log(`📊 DATABASE_URL configured: ${process.env.DATABASE_URL ? 'YES' : 'NO'}`);
    
    return new Promise((resolve, reject) => {
        const migrate = spawn('npx', ['prisma', 'migrate', 'deploy'], {
            stdio: 'inherit',
            env: process.env
        });
        
        migrate.on('close', (code) => {
            const completeTimestamp = new Date().toISOString();
            if (code === 0) {
                console.log(`✅ [${completeTimestamp}] Prisma migrations completed successfully`);
                console.log('📋 Migration status: All pending migrations have been applied');
                resolve(true);
            } else {
                console.error(`❌ [${completeTimestamp}] Prisma migrations failed with exit code ${code}`);
                console.error(`🔍 Troubleshooting: Check DATABASE_URL, network connectivity, and Postgres permissions`);
                reject(new Error(`Migration failed with code ${code}`));
            }
        });
        
        migrate.on('error', (error) => {
            const errorTimestamp = new Date().toISOString();
            console.error(`❌ [${errorTimestamp}] Failed to run Prisma migrations:`, error.message);
            console.error(`🔍 Error details:`, JSON.stringify({
                message: error.message,
                code: error.code,
                stack: error.stack?.split('\n')[0]
            }));
            reject(error);
        });
    });
}

// Generate Prisma client
async function generatePrismaClient() {
    console.log('🔧 Generating Prisma client...');
    
    return new Promise((resolve, reject) => {
        const generate = spawn('npx', ['prisma', 'generate'], {
            stdio: 'inherit',
            env: process.env
        });
        
        generate.on('close', (code) => {
            if (code === 0) {
                console.log('✅ Prisma client generated successfully');
                resolve(true);
            } else {
                console.error(`❌ Prisma client generation failed with exit code ${code}`);
                reject(new Error(`Client generation failed with code ${code}`));
            }
        });
        
        generate.on('error', (error) => {
            console.error('❌ Failed to generate Prisma client:', error.message);
            reject(error);
        });
    });
}

// For this project, "migration" means ensuring data directories exist
function ensureDataDirectories() {
    console.log('📁 Ensuring data directories exist...');
    
    const dataDir = path.join(__dirname, '..', 'data');
    const participantsDir = path.join(dataDir, 'participants');
    const conversationsDir = path.join(dataDir, 'conversations');
    const exportsDir = path.join(dataDir, 'exports');
    const endSurveysDir = path.join(dataDir, 'end-surveys');
    
    const directories = [dataDir, participantsDir, conversationsDir, exportsDir, endSurveysDir];
    
    try {
        directories.forEach(dir => {
            if (!fs.existsSync(dir)) {
                fs.mkdirSync(dir, { recursive: true });
                console.log(`✅ Created directory: ${dir}`);
            } else {
                console.log(`✅ Directory exists: ${dir}`);
            }
        });
        
        console.log('✅ All data directories ready');
        return true;
    } catch (error) {
        console.error('❌ Failed to create data directories:', error.message);
        return false;
    }
}

// Check required environment variables
function checkEnvironmentVariables() {
    console.log('🔍 Checking environment variables...');
    
    const requiredEnvVars = ['NODE_ENV'];
    const optionalEnvVars = ['OPENAI_API_KEY', 'WEB_ORIGIN', 'ADMIN_TOKEN', 'PORT', 'DATABASE_URL'];
    
    let hasErrors = false;
    
    // Check required vars
    requiredEnvVars.forEach(envVar => {
        if (!process.env[envVar]) {
            console.error(`❌ Missing required environment variable: ${envVar}`);
            hasErrors = true;
        } else {
            console.log(`✅ ${envVar}: ${envVar === 'NODE_ENV' ? process.env[envVar] : '[SET]'}`);
        }
    });
    
    // Check optional vars (just log status)
    optionalEnvVars.forEach(envVar => {
        if (process.env[envVar]) {
            console.log(`✅ ${envVar}: [SET]`);
        } else {
            console.log(`⚠️  ${envVar}: [NOT SET]`);
        }
    });
    
    if (hasErrors) {
        console.error('❌ Environment validation failed');
        return false;
    }
    
    console.log('✅ Environment variables validated');
    return true;
}

// Start the production server
function startServer() {
    console.log('🚀 Starting production server...');
    
    const serverPath = path.join(__dirname, '..', 'server.js');
    
    // Spawn the server process
    const server = spawn('node', [serverPath], {
        stdio: 'inherit',
        env: process.env
    });
    
    server.on('error', (error) => {
        console.error('❌ Failed to start server:', error.message);
        process.exit(1);
    });
    
    server.on('exit', (code) => {
        console.log(`Server exited with code ${code}`);
        process.exit(code);
    });
    
    // Handle termination signals
    process.on('SIGTERM', () => {
        console.log('Received SIGTERM, shutting down gracefully');
        server.kill('SIGTERM');
    });
    
    process.on('SIGINT', () => {
        console.log('Received SIGINT, shutting down gracefully');
        server.kill('SIGINT');
    });
}

// Main execution
async function main() {
    try {
        // Step 1: Check environment variables
        if (!checkEnvironmentVariables()) {
            process.exit(1);
        }
        
        // Step 2: Generate Prisma client
        await generatePrismaClient();
        
        // Step 3: Run database migrations (if DATABASE_URL is available)
        if (process.env.DATABASE_URL) {
            try {
                await runPrismaMigrations();
            } catch (error) {
                if (process.env.NODE_ENV === 'production') {
                    console.error('❌ Database migrations failed in production');
                    console.error('Error:', error.message);
                    process.exit(1); // Fatal error in production
                }
                console.warn('⚠️ Database migrations failed, continuing without database:', error.message);
            }
        } else {
            console.log('⚠️ DATABASE_URL not set, skipping database migrations');
        }
        
        // Step 4: Ensure data directories exist (fallback storage)
        if (!ensureDataDirectories()) {
            process.exit(1);
        }
        
        // Step 5: Start the server
        console.log('🎉 Migration completed successfully, starting server...');
        startServer();
        
    } catch (error) {
        console.error('❌ Migration failed:', error.message);
        process.exit(1);
    }
}

main();