const { PrismaClient } = require('@prisma/client');
const fs = require('fs');
const path = require('path');

// Initialize Prisma client
let prisma = null;
let isInitialized = false;

// File storage fallback directories
const dataDir = path.join(__dirname, 'data');
const participantsDir = path.join(dataDir, 'participants');
const conversationsDir = path.join(dataDir, 'conversations');

// Initialize Prisma client
function initializePrisma() {
    if (prisma) return prisma;
    
    if (!process.env.DATABASE_URL) {
        if (process.env.NODE_ENV === 'production') {
            throw new Error('❌ DATABASE_URL is required in production. File storage not allowed.');
        }
        console.log('⚠️ DATABASE_URL not configured, using file fallback (development only)');
        return null;
    }
    
    try {
        prisma = new PrismaClient({
            log: process.env.NODE_ENV === 'development' ? ['warn', 'error'] : ['error']
        });
        console.log('✅ Prisma client initialized');
        return prisma;
    } catch (error) {
        console.error('❌ Failed to initialize Prisma client:', error.message);
        return null;
    }
}

// Test database availability
async function isDatabaseAvailable() {
    if (!prisma) {
        prisma = initializePrisma();
    }
    
    if (!prisma) return false;
    
    try {
        await prisma.$queryRaw`SELECT 1`;
        return true;
    } catch (error) {
        console.log('❌ Database availability check failed:', error.message);
        return false;
    }
}

// Initialize database (Prisma-based, no custom schema creation)
async function initializeDatabase() {
    if (isInitialized) {
        console.log('✅ Database already initialized (using Prisma)');
        return true;
    }
    
    if (!await isDatabaseAvailable()) {
        console.log('⚠️ Database unavailable, skipping initialization');
        return false;
    }
    
    try {
        // Test Prisma connection and ensure tables exist
        await prisma.$connect();
        
        // Quick test to ensure Prisma tables are accessible
        await prisma.session.findFirst().catch(() => {
            // If this fails, Prisma schema hasn't been deployed
            console.log('⚠️ Prisma schema may need deployment: npx prisma migrate deploy');
        });
        
        console.log('✅ Prisma database connection established');
        isInitialized = true;
        return true;
        
    } catch (error) {
        console.error('❌ Prisma database initialization failed:', error.message);
        return false;
    }
}

// Utility functions for file fallback
function writeJson(filePath, obj) {
    try {
        const dir = path.dirname(filePath);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }
        fs.writeFileSync(filePath, JSON.stringify(obj, null, 2));
        return true;
    } catch (error) {
        console.error('❌ Error writing JSON file:', error);
        return false;
    }
}

function readJson(filePath) {
    try {
        if (!fs.existsSync(filePath)) {
            return null;
        }
        const data = fs.readFileSync(filePath, 'utf8');
        return JSON.parse(data);
    } catch (error) {
        console.error('❌ Error reading JSON file:', error);
        return null;
    }
}

// Data transformation functions
function transformPrismaToParticipant(individualDiff) {
    if (!individualDiff || !individualDiff.raw) return null;
    
    const raw = individualDiff.raw;
    const session = individualDiff.session;
    
    return {
        participant_id: raw.participant_id || session?.id,
        prolific_id: raw.prolific_id,
        demographics: raw.demographics,
        belief_change: raw.belief_change,
        views_matrix: raw.views_matrix,
        chatbot_interaction: {
            messages: session?.messages ? session.messages.map(transformPrismaToMessage) : []
        },
        post_chat: raw.post_chat,
        timestamps: raw.timestamps,
        // Legacy compatibility
        id: session?.id || individualDiff.sessionId,
        createdAt: session?.createdAt,
        updatedAt: session?.updatedAt
    };
}

function transformPrismaToSession(session) {
    return {
        id: session.id,
        participantId: session.participantId,
        startedAt: session.startedAt,
        endedAt: session.completedAt,
        durationSeconds: session.raw?.durationSeconds,
        systemPrompt: session.raw?.systemPrompt,
        messages: session.messages ? session.messages.map(transformPrismaToMessage) : [],
        raw_data: session.raw
    };
}

function transformPrismaToMessage(message) {
    return {
        message_id: message.id,
        session_id: message.sessionId,
        participant_id: message.session?.participantId,
        turn_number: message.turn,
        role: message.role,
        content: message.content,
        message_timestamp: message.timestamp,
        character_count: message.content ? message.content.length : 0
    };
}

// File fallback functions
function getParticipantsFromFiles() {
    try {
        if (!fs.existsSync(participantsDir)) {
            return [];
        }
        const files = fs.readdirSync(participantsDir)
            .filter(file => file.endsWith('.json'))
            .map(file => readJson(path.join(participantsDir, file)))
            .filter(data => data);
        console.log(`✅ Retrieved ${files.length} participants from files`);
        return files;
    } catch (error) {
        console.error('❌ File fallback failed for participants:', error);
        return [];
    }
}

function getSessionsFromFiles() {
    try {
        if (!fs.existsSync(conversationsDir)) {
            return [];
        }
        const files = fs.readdirSync(conversationsDir)
            .filter(file => file.endsWith('.json'))
            .map(file => readJson(path.join(conversationsDir, file)))
            .filter(data => data);
        console.log(`✅ Retrieved ${files.length} sessions from files`);
        return files;
    } catch (error) {
        console.error('❌ File fallback failed for sessions:', error);
        return [];
    }
}

function getMessagesFromFiles() {
    try {
        if (!fs.existsSync(conversationsDir)) {
            return [];
        }
        
        const messages = [];
        const files = fs.readdirSync(conversationsDir).filter(file => file.endsWith('.json'));
        
        for (const file of files) {
            const session = readJson(path.join(conversationsDir, file));
            if (session && session.messages) {
                session.messages.forEach((msg, index) => {
                    messages.push({
                        message_id: `${session.id}-msg-${index}`,
                        session_id: session.id,
                        participant_id: session.participantId,
                        turn_number: index,
                        role: msg.role,
                        content: msg.content,
                        message_timestamp: msg.timestamp,
                        character_count: msg.content ? msg.content.length : 0
                    });
                });
            }
        }
        
        console.log(`✅ Extracted ${messages.length} messages from session files`);
        return messages;
    } catch (error) {
        console.error('❌ File fallback failed for messages:', error);
        return [];
    }
}

// CRUD Functions using Prisma

// Get all participants
async function getAllParticipants() {
    if (!await isDatabaseAvailable()) {
        console.log('❌ Database unavailable, using file fallback for participants');
        return getParticipantsFromFiles();
    }
    
    try {
        const individualDifferences = await prisma.individualDifferences.findMany({
            include: {
                session: {
                    include: {
                        messages: true
                    }
                }
            }
        });
        
        console.log(`✅ Retrieved ${individualDifferences.length} participants from Prisma`);
        return individualDifferences.map(transformPrismaToParticipant).filter(p => p);
        
    } catch (error) {
        console.error('❌ Prisma participants query failed:', error.message);
        return getParticipantsFromFiles(); // Fallback to files
    }
}

// Get all sessions
async function getAllSessions() {
    if (!await isDatabaseAvailable()) {
        console.log('❌ Database unavailable, using file fallback for sessions');
        return getSessionsFromFiles();
    }
    
    try {
        const sessions = await prisma.session.findMany({
            include: {
                messages: true,
                individualDifferences: true
            },
            orderBy: { createdAt: 'desc' }
        });
        
        console.log(`✅ Retrieved ${sessions.length} sessions from Prisma`);
        return sessions.map(transformPrismaToSession);
        
    } catch (error) {
        console.error('❌ Prisma sessions query failed:', error.message);
        return getSessionsFromFiles(); // Fallback to files
    }
}

// Get all messages
async function getAllMessages() {
    if (!await isDatabaseAvailable()) {
        console.log('❌ Database unavailable, extracting messages from session files');
        return getMessagesFromFiles();
    }
    
    try {
        const messages = await prisma.message.findMany({
            orderBy: [
                { sessionId: 'asc' },
                { turn: 'asc' }
            ]
        });
        
        console.log(`✅ Retrieved ${messages.length} messages from Prisma`);
        return messages.map(transformPrismaToMessage);
        
    } catch (error) {
        console.error('❌ Prisma messages query failed:', error.message);
        return getMessagesFromFiles(); // Fallback to files
    }
}

// Save participant (backward compatibility)
async function saveParticipant(data) {
    if (!await isDatabaseAvailable()) {
        console.log('❌ Database unavailable, using file fallback for participant save');
        const filename = path.join(participantsDir, `${data.participant_id}.json`);
        const success = writeJson(filename, data);
        console.log(`${success ? '✅' : '❌'} Participant ${data.participant_id} saved to file`);
        return success ? data : null;
    }
    
    try {
        // In Prisma schema, participant data is stored in IndividualDifferences
        // We need to create or update both Session and IndividualDifferences
        
        const sessionData = {
            id: data.id || data.participant_id,
            participantId: data.participant_id,
            startedAt: data.timestamps?.started ? new Date(data.timestamps.started) : new Date(),
            completedAt: data.timestamps?.completed ? new Date(data.timestamps.completed) : null,
            raw: data
        };
        
        // Upsert session
        const session = await prisma.session.upsert({
            where: { id: sessionData.id },
            update: sessionData,
            create: sessionData
        });
        
        // Enhanced data mapping with proper type conversion and error handling
        const mappedData = {
            raw: data,
            
            // Basic demographics with proper type conversion
            age: data.demographics?.age ? parseInt(data.demographics.age) : null,
            gender: data.demographics?.gender || null,
            education: data.demographics?.education || null,
            
            // Belief change data with proper type conversion
            viewsChanged: data.belief_change?.has_changed_mind ? 'Yes' : 'No',
            mindChangeDirection: data.belief_change?.mind_change_direction || null,
            mindChangeNoChange: Boolean(data.belief_change?.mind_change_no_change),
            mindChangeOtherText: data.belief_change?.mind_change_other_text || null,
            
            // CCS scale means with proper type conversion
            ccsMeanScored: data.views_matrix?.climate_change_views?.ccs_mean_scored ?
                parseFloat(data.views_matrix.climate_change_views.ccs_mean_scored) : null,
            ccsOccurrenceMean: data.views_matrix?.climate_change_views?.ccs_occurrence_mean ?
                parseFloat(data.views_matrix.climate_change_views.ccs_occurrence_mean) : null,
            ccsCausationMean: data.views_matrix?.climate_change_views?.ccs_causation_mean ?
                parseFloat(data.views_matrix.climate_change_views.ccs_causation_mean) : null,
            ccsSeriousnessMean: data.views_matrix?.climate_change_views?.ccs_seriousness_mean ?
                parseFloat(data.views_matrix.climate_change_views.ccs_seriousness_mean) : null,
            ccsEfficacyMean: data.views_matrix?.climate_change_views?.ccs_efficacy_mean ?
                parseFloat(data.views_matrix.climate_change_views.ccs_efficacy_mean) : null,
            ccsTrustMean: data.views_matrix?.climate_change_views?.ccs_trust_mean ?
                parseFloat(data.views_matrix.climate_change_views.ccs_trust_mean) : null,
            
            // Political views with proper type conversion
            economicIssues: data.views_matrix?.political_views?.economic_issues ?
                parseInt(data.views_matrix.political_views.economic_issues) : null,
            socialIssues: data.views_matrix?.political_views?.social_issues ?
                parseInt(data.views_matrix.political_views.social_issues) : null,
            
            // AI Summary data
            aiSummary: data.belief_change?.ai_summary || null,
            aiSummaryAccuracy: data.belief_change?.ai_summary_accuracy ||
                data.post_chat?.chatbot_summary_accuracy || null,
            aiConfidenceSlider: data.belief_change?.ai_confidence_slider ?
                parseInt(data.belief_change.ai_confidence_slider) : null,
            
            // Survey completion tracking
            surveyCompleted: Boolean(data.timestamps?.completed),
            completedAt: data.timestamps?.completed ? new Date(data.timestamps.completed) : null,
            prolificId: data.prolific_id || null
        };

        console.log('💾 Database mapping for participant:', data.participant_id, {
            hasAge: !!mappedData.age,
            hasGender: !!mappedData.gender,
            hasEconomicIssues: !!mappedData.economicIssues,
            hasCcsMean: !!mappedData.ccsMeanScored,
            hasAiSummary: !!mappedData.aiSummary,
            hasProlificId: !!mappedData.prolificId
        });

        // Upsert individual differences
        const individualDiff = await prisma.individualDifferences.upsert({
            where: { sessionId: session.id },
            update: mappedData,
            create: {
                sessionId: session.id,
                ...mappedData
            }
        });
        
        console.log('✅ Participant saved to Prisma database:', data.participant_id);
        
        // Also save to file as backup in development
        if (process.env.NODE_ENV !== 'production') {
            const filename = path.join(participantsDir, `${data.participant_id}.json`);
            writeJson(filename, data);
        }
        
        return data;
        
    } catch (error) {
        console.error('❌ Prisma participant save failed:', error.message);
        // Fallback to file storage
        const filename = path.join(participantsDir, `${data.participant_id}.json`);
        const success = writeJson(filename, data);
        console.log(`${success ? '✅' : '❌'} Participant ${data.participant_id} saved to file fallback`);
        return success ? data : null;
    }
}

// Save session (backward compatibility)
async function saveSession(data) {
    if (!await isDatabaseAvailable()) {
        console.log('❌ Database unavailable, using file fallback for session save');
        const filename = path.join(conversationsDir, `${data.id || data.session_id}.json`);
        const success = writeJson(filename, data);
        console.log(`${success ? '✅' : '❌'} Session ${data.id || data.session_id} saved to file`);
        return success ? data : null;
    }
    
    try {
        const sessionId = data.id || data.session_id;
        
        const sessionData = {
            id: sessionId,
            participantId: data.participantId || data.participant_id,
            startedAt: data.startedAt ? new Date(data.startedAt) : null,
            completedAt: data.endedAt ? new Date(data.endedAt) : null,
            raw: data
        };
        
        // Upsert session
        const session = await prisma.session.upsert({
            where: { id: sessionId },
            update: sessionData,
            create: sessionData
        });
        
        // Save messages if they exist
        if (data.messages && data.messages.length > 0) {
            await saveMessages(sessionId, data.messages);
        }
        
        console.log('✅ Session saved to Prisma database:', sessionId);
        
        // Also save to file as backup in development
        if (process.env.NODE_ENV !== 'production') {
            const filename = path.join(conversationsDir, `${sessionId}.json`);
            writeJson(filename, data);
        }
        
        return data;
        
    } catch (error) {
        console.error('❌ Prisma session save failed:', error.message);
        // Fallback to file storage
        const filename = path.join(conversationsDir, `${data.id || data.session_id}.json`);
        const success = writeJson(filename, data);
        console.log(`${success ? '✅' : '❌'} Session ${data.id || data.session_id} saved to file fallback`);
        return success ? data : null;
    }
}

// Save messages
async function saveMessages(sessionId, messages) {
    if (!await isDatabaseAvailable()) {
        console.log('❌ Database unavailable, skipping messages save (stored with sessions in files)');
        return messages;
    }
    
    try {
        // Delete existing messages for this session first
        await prisma.message.deleteMany({
            where: { sessionId: sessionId }
        });
        
        // Insert all messages
        const messagePromises = messages.map((msg, index) => {
            return prisma.message.create({
                data: {
                    id: `${sessionId}-msg-${index}`,
                    sessionId: sessionId,
                    turn: index,
                    role: msg.role,
                    content: msg.content,
                    timestamp: msg.timestamp ? new Date(msg.timestamp) : new Date()
                }
            });
        });
        
        await Promise.all(messagePromises);
        console.log(`✅ ${messages.length} messages saved to Prisma for session ${sessionId}`);
        return messages;
        
    } catch (error) {
        console.error('❌ Prisma messages save failed:', error.message);
        return messages;
    }
}

// Database statistics
async function getDatabaseStats() {
    if (!await isDatabaseAvailable()) {
        console.log('❌ Database unavailable, calculating stats from files');
        try {
            const participantCount = fs.existsSync(participantsDir) 
                ? fs.readdirSync(participantsDir).filter(f => f.endsWith('.json')).length 
                : 0;
            const sessionCount = fs.existsSync(conversationsDir) 
                ? fs.readdirSync(conversationsDir).filter(f => f.endsWith('.json')).length 
                : 0;

            return {
                source: 'files',
                tables: {
                    participants: { count: participantCount, size_mb: 0 },
                    sessions: { count: sessionCount, size_mb: 0 },
                    messages: { count: 0, size_mb: 0 }
                },
                connection_pool: {
                    total: 0,
                    idle: 0,
                    waiting: 0
                }
            };
        } catch (error) {
            console.error('❌ File stats calculation failed:', error);
            return null;
        }
    }

    try {
        // Get counts using Prisma
        const [participantCount, sessionCount, messageCount] = await Promise.all([
            prisma.individualDifferences.count(),
            prisma.session.count(),
            prisma.message.count()
        ]);
        
        const stats = {
            source: 'database',
            tables: {
                participants: { count: participantCount, size_mb: 0 },
                sessions: { count: sessionCount, size_mb: 0 },
                messages: { count: messageCount, size_mb: 0 }
            },
            connection_pool: {
                total: 1, // Prisma manages its own connection pool
                idle: 0,
                waiting: 0
            }
        };

        console.log('✅ Database statistics calculated');
        return stats;

    } catch (error) {
        console.error('❌ Database stats calculation failed:', error.message);
        return null;
    }
}

// Clear all data (DANGEROUS - admin only)
async function clearAllData() {
    if (!await isDatabaseAvailable()) {
        console.log('❌ Database unavailable, clearing file storage only');
        return clearFileStorage();
    }
    
    const startTime = Date.now();
    const summary = {
        database: { messages: 0, sessions: 0, participants: 0 },
        files: { participants: 0, sessions: 0 },
        duration_ms: 0
    };
    
    try {
        // Clear database in proper order (due to foreign key constraints)
        console.log('🗑️ Starting database clear operation...');
        
        // 1. Clear messages first
        const deletedMessages = await prisma.message.deleteMany();
        summary.database.messages = deletedMessages.count;
        console.log(`🗑️ Cleared ${deletedMessages.count} messages`);
        
        // 2. Clear individual differences
        const deletedParticipants = await prisma.individualDifferences.deleteMany();
        summary.database.participants = deletedParticipants.count;
        console.log(`🗑️ Cleared ${deletedParticipants.count} participant records`);
        
        // 3. Clear sessions last
        const deletedSessions = await prisma.session.deleteMany();
        summary.database.sessions = deletedSessions.count;
        console.log(`🗑️ Cleared ${deletedSessions.count} sessions`);
        
        // 4. Clear file storage as well
        const filesSummary = clearFileStorage();
        summary.files = filesSummary;
        
        summary.duration_ms = Date.now() - startTime;
        
        console.log(`✅ Database clear completed in ${summary.duration_ms}ms`);
        console.log(`📊 Total cleared: ${summary.database.messages} messages, ${summary.database.sessions} sessions, ${summary.database.participants} participants`);
        
        return {
            success: true,
            cleared_at: new Date().toISOString(),
            summary: summary
        };
        
    } catch (error) {
        console.error('❌ Database clear failed:', error.message);
        
        // If database clear fails, still clear files
        const filesSummary = clearFileStorage();
        
        return {
            success: false,
            error: error.message,
            files_cleared: filesSummary,
            duration_ms: Date.now() - startTime
        };
    }
}

// Clear file storage
function clearFileStorage() {
    const summary = { participants: 0, sessions: 0 };
    
    try {
        // Clear participant files
        if (fs.existsSync(participantsDir)) {
            const participantFiles = fs.readdirSync(participantsDir).filter(f => f.endsWith('.json'));
            participantFiles.forEach(file => {
                fs.unlinkSync(path.join(participantsDir, file));
            });
            summary.participants = participantFiles.length;
            console.log(`🗑️ Cleared ${participantFiles.length} participant files`);
        }
        
        // Clear conversation files
        if (fs.existsSync(conversationsDir)) {
            const conversationFiles = fs.readdirSync(conversationsDir).filter(f => f.endsWith('.json'));
            conversationFiles.forEach(file => {
                fs.unlinkSync(path.join(conversationsDir, file));
            });
            summary.sessions = conversationFiles.length;
            console.log(`🗑️ Cleared ${conversationFiles.length} conversation files`);
        }
        
        // Clear exports directory
        const exportsDir = path.join(dataDir, 'exports');
        if (fs.existsSync(exportsDir)) {
            const exportFiles = fs.readdirSync(exportsDir).filter(f => f.endsWith('.json'));
            exportFiles.forEach(file => {
                fs.unlinkSync(path.join(exportsDir, file));
            });
            console.log(`🗑️ Cleared ${exportFiles.length} export files`);
        }
        
    } catch (error) {
        console.error('❌ File storage clear failed:', error.message);
        summary.error = error.message;
    }
    
    return summary;
}

// Close database connections gracefully
async function closeDatabase() {
    if (prisma) {
        try {
            await prisma.$disconnect();
            console.log('✅ Prisma client disconnected');
            prisma = null;
            isInitialized = false;
        } catch (error) {
            console.error('❌ Error disconnecting Prisma client:', error.message);
        }
    }
}

// Get participant by ID (backward compatibility)
async function getParticipant(participantId) {
    if (!await isDatabaseAvailable()) {
        const filename = path.join(participantsDir, `${participantId}.json`);
        return readJson(filename);
    }
    
    try {
        const individualDiff = await prisma.individualDifferences.findFirst({
            where: {
                OR: [
                    { sessionId: participantId },
                    { raw: { path: ['participant_id'], equals: participantId } }
                ]
            },
            include: {
                session: {
                    include: {
                        messages: true
                    }
                }
            }
        });
        
        return individualDiff ? transformPrismaToParticipant(individualDiff) : null;
        
    } catch (error) {
        console.error('❌ Prisma getParticipant failed:', error.message);
        const filename = path.join(participantsDir, `${participantId}.json`);
        return readJson(filename);
    }
}

// Get session by ID (backward compatibility)
async function getSession(sessionId) {
    if (!await isDatabaseAvailable()) {
        const filename = path.join(conversationsDir, `${sessionId}.json`);
        return readJson(filename);
    }
    
    try {
        const session = await prisma.session.findUnique({
            where: { id: sessionId },
            include: {
                messages: true,
                individualDifferences: true
            }
        });
        
        return session ? transformPrismaToSession(session) : null;
        
    } catch (error) {
        console.error('❌ Prisma getSession failed:', error.message);
        const filename = path.join(conversationsDir, `${sessionId}.json`);
        return readJson(filename);
    }
}

// Initialize Prisma on module load
prisma = initializePrisma();

// Export all functions
module.exports = {
    isDatabaseAvailable,
    initializeDatabase,
    
    // Participant functions
    saveParticipant,
    getParticipant,
    getAllParticipants,
    
    // Session functions
    saveSession,
    getSession,
    getAllSessions,
    
    // Message functions
    saveMessages,
    getAllMessages,
    
    // Utility functions
    getDatabaseStats,
    clearAllData,
    closeDatabase
};