const express = require('express');
const path = require('path');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');
const cors = require('cors');
const OpenAI = require('openai');
const { dataAccess } = require('./lib/dataAccess');
const database = require('./database');
require('dotenv').config();

// Enhanced chat router will be imported and mounted below

// Chat duration constant - 5 minutes
const CHAT_DURATION_MS = Number(process.env.CHAT_DURATION_MS ?? 5 * 60 * 1000);

const app = express();

// Middleware
// Configure CORS based on environment
const corsOptions = {
  origin: function (origin, callback) {
    // Allow requests with no origin (like mobile apps or curl requests)
    if (!origin) return callback(null, true);
    
    // In development, allow all origins
    if (process.env.NODE_ENV !== 'production') {
      return callback(null, true);
    }
    
    // In production, check WEB_ORIGIN environment variable
    const allowedOrigins = process.env.WEB_ORIGIN ? process.env.WEB_ORIGIN.split(',') : [];
    
    if (allowedOrigins.includes(origin)) {
      return callback(null, true);
    } else {
      console.warn(`CORS blocked request from origin: ${origin}`);
      return callback(new Error('Not allowed by CORS'), false);
    }
  },
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'x-admin-token'],
  optionsSuccessStatus: 200 // For legacy browser support
};

app.use(cors(corsOptions));

// Production logging middleware for chat endpoints
function chatLogger(req, res, next) {
  if (process.env.NODE_ENV === 'production' && req.path.startsWith('/api/conversations')) {
    const start = Date.now();
    const originalSend = res.send;
    
    res.send = function(data) {
      const duration = Date.now() - start;
      const responseSize = Buffer.byteLength(data, 'utf8');
      console.log(`[CHAT] ${req.method} ${req.path} - ${res.statusCode} - ${duration}ms - ${responseSize}b - IP: ${req.ip}`);
      return originalSend.call(this, data);
    };
  }
  next();
}

app.use(chatLogger);
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

// Data storage directories
const dataDir = path.join(__dirname, 'data');
const participantsDir = path.join(dataDir, 'participants');
const conversationsDir = path.join(dataDir, 'conversations');
const exportsDir = path.join(dataDir, 'exports');

// Create data directories if they don't exist
[dataDir, participantsDir, conversationsDir, exportsDir].forEach(dir => {
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }
});

// Ensure core data directories exist with logging
[participantsDir, conversationsDir].forEach(dir => {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
    console.log('Created data dir:', dir);
  }
});

// In-memory storage for active conversations
const activeConversations = new Map();

// Global database and LLM shims for new chat functionality
global.db = global.db || {
  participants: {
    async getProfile(userId) {
      const filename = path.join(participantsDir, `${userId}.json`);
      const participant = readJson(filename);
      if (!participant) return null;
      
      // Map existing fields to expected structure for chat system
      const hasChangedMind = participant.belief_change?.has_changed_mind;
      const viewsChanged = hasChangedMind ? 'Yes' : (hasChangedMind === false ? 'No' : 'unspecified');
      
      // Map the specific belief change direction from survey
      const mindChangeDirection = participant.belief_change?.mind_change_direction;
      const mindChangeOtherText = participant.belief_change?.mind_change_other_text;
      
      // Convert the direction code to a descriptive text for the system prompt
      let changeDescription = null;
      if (mindChangeDirection) {
        switch (mindChangeDirection) {
          case 'exists_to_not_exists':
            changeDescription = 'From thinking climate change exists, to thinking climate change does not exist';
            break;
          case 'not_exists_to_exists':
            changeDescription = 'From thinking climate change does not exist, to thinking climate change exists';
            break;
          case 'not_urgent_to_urgent':
            changeDescription = 'From thinking climate change is not an urgent crisis, to thinking climate change is an urgent crisis';
            break;
          case 'urgent_to_not_urgent':
            changeDescription = 'From thinking climate change is an urgent crisis, to thinking climate change is not an urgent crisis';
            break;
          case 'human_to_natural':
            changeDescription = 'From thinking climate change is primarily caused by human activity, to thinking climate change is a largely natural process';
            break;
          case 'natural_to_human':
            changeDescription = 'From thinking climate change is a largely natural process, to thinking climate change is primarily caused by human activity';
            break;
          case 'other':
            changeDescription = mindChangeOtherText || 'Other belief change (details provided by participant)';
            break;
          default:
            changeDescription = null;
        }
      }
      
      return {
        views_changed: viewsChanged,
        change_description: changeDescription,
        change_confidence: null, // Not collected in current survey flow
        mind_change_direction: mindChangeDirection,
        mind_change_other_text: mindChangeOtherText,
        prior_belief_cc_happening: null,
        prior_belief_human_cause: "unspecified",
        current_belief_cc_happening: null,
        current_belief_human_cause: "unspecified",
        changed_belief_flag: hasChangedMind === true
      };
    },
    async updateFromConversation(conversationId, updates) {
      const filename = path.join(conversationsDir, `${conversationId}.json`);
      const conv = readJson(filename);
      if (!conv) return;
      
      const participantFile = path.join(participantsDir, `${conv.participantId}.json`);
      const participant = readJson(participantFile);
      if (!participant) return;
      
      // Update participant with new fields
      Object.assign(participant, updates);
      participant.updatedAt = new Date().toISOString();
      writeJson(participantFile, participant);
    }
  },
  conversations: {
    async save(userId, conversationId, messages) {
      const conversationData = {
        id: conversationId,
        participantId: userId,
        startedAt: new Date().toISOString(),
        endedAt: null,
        durationSeconds: null,
        messages: messages
      };
      
      // Use data access layer for dual-write
      await dataAccess.saveSession(conversationData);
    },
    async load(conversationId) {
      const filename = path.join(conversationsDir, `${conversationId}.json`);
      const c = readJson(filename);
      return c?.messages || [];
    },
    async append(conversationId, message) {
      const filename = path.join(conversationsDir, `${conversationId}.json`);
      const c = readJson(filename);
      if (!c) return;
      
      const newMsgs = [...(c.messages || []), message];
      c.messages = newMsgs;
      
      // Update with data access layer
      const updatedConversation = { ...c, messages: newMsgs };
      await dataAccess.saveSession(updatedConversation);
    }
  }
};

global.llm = global.llm || {
  async chat(messages) {
    // Use existing OpenAI integration
    if (!process.env.OPENAI_API_KEY) {
      return { content: "I'm here to help you explore your thoughts about climate change. Could you tell me more about your perspective?" };
    }
    
    try {
      const completion = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: messages,
        max_tokens: 150,
        temperature: 0.7
      });
      
      return { content: completion.choices[0]?.message?.content?.trim() || "Could you tell me more about your thoughts?" };
    } catch (error) {
      console.error('LLM error:', error);
      return { content: "Could you tell me more about your thoughts on climate change?" };
    }
  }
};

// Utility functions
function writeJson(filePath, obj) {
    try {
        fs.writeFileSync(filePath, JSON.stringify(obj, null, 2));
        return true;
    } catch (error) {
        console.error('Error writing JSON file:', error);
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
        console.error('Error reading JSON file:', error);
        return null;
    }
}

// Routes
// Enhanced chat router mounting with better error handling
(async () => {
  try {
    // Try to import and mount the enhanced chat router
    const module = await import('./backend/src/routes/chat.js');
    const chatRouter = module.default;
    
    if (chatRouter) {
      app.use("/chat", chatRouter);
      console.log('✅ Enhanced chat router successfully mounted at /chat');
    } else {
      throw new Error('Chat router module did not export default router');
    }
  } catch (error) {
    console.error('❌ Failed to load enhanced chat router:', error.message);
    console.log('🔄 Using fallback chat endpoints');
    
    // Fallback chat endpoints when enhanced router is unavailable
    app.post('/chat/start', async (req, res) => {
      try {
        const { userId } = req.body || {};
        const participantId = userId || uuidv4();
        const conversationId = uuidv4();

        const now = new Date().toISOString();
        const initial = {
          id: conversationId,
          participantId,
          startedAt: now,
          endedAt: null,
          durationSeconds: 0,
          messages: []
        };

        // Use data access layer for dual-write
        await dataAccess.saveSession(initial);

        activeConversations.set(conversationId, { startedAt: now });

        // Use enhanced opening line logic even in fallback
        const profile = await global.db.participants.getProfile(participantId);
        const { openingLineFrom } = await import('./backend/src/utils/openingLine.js');
        const opening = openingLineFrom(profile);
        
        initial.messages.push({ role: "assistant", content: opening });
        await dataAccess.saveSession(initial);

        res.json({ conversationId, messages: initial.messages });
      } catch (err) {
        console.error('fallback /chat/start error', err);
        res.status(500).json({ error: 'Failed to start chat' });
      }
    });

    // Reply endpoint with enhanced system prompt
    app.post('/chat/reply', async (req, res) => {
      try {
        const { conversationId, message } = req.body || {};
        if (!conversationId || !message || !message.trim()) {
          return res.status(400).json({ error: 'conversationId and message are required' });
        }

        const activeConv = activeConversations.get(conversationId);
        if (!activeConv) {
          return res.status(404).json({ error: 'Conversation not found or ended' });
        }

        const now = new Date();
        const startTime = new Date(activeConv.startedAt);
        const elapsedSeconds = Math.floor((now - startTime) / 1000);
        if (elapsedSeconds >= (CHAT_DURATION_MS / 1000)) {
          activeConversations.delete(conversationId);
          return res.status(410).json({ error: 'Conversation time limit exceeded' });
        }

        // Early-end phrase
        if (shouldEndNow(message)) {
          const filename = path.join(conversationsDir, `${conversationId}.json`);
          const conv = readJson(filename) || {};
          conv.messages = conv.messages || [];
          conv.messages.push({ role: "user", content: message });
          
          // Ensure conversation has a summary before ending
          if (conv.participantId) {
            console.log('⚠️ Early chat end detected in fallback endpoint, ensuring summary exists');
            await fallbackEnsureConversationSummary(conversationId, conv.participantId);
            // Reload conversation data after potential summary addition
            const updatedConv = readJson(filename);
            if (updatedConv) {
              conv.messages = updatedConv.messages;
            }
          }
          
          const finalReply = "Okay — ending the chat now. Thanks for participating!";
          conv.messages.push({ role: "assistant", content: finalReply });
          conv.endedAt = now.toISOString();
          conv.durationSeconds = Math.floor((now - startTime) / 1000);
          await dataAccess.saveSession(conv);
          activeConversations.delete(conversationId);
          return res.json({ reply: finalReply, sessionEnded: true });
        }

        // Normal AI reply with enhanced system prompt
        const filename = path.join(conversationsDir, `${conversationId}.json`);
        const conv = readJson(filename) || {};
        conv.messages = conv.messages || [];
        conv.messages.push({ role: "user", content: message });

        // Get participant profile and use enhanced system prompt
        const profile = await global.db.participants.getProfile(conv.participantId);
        const { renderSystemPrompt } = await import('./backend/src/utils/systemPrompt.js');
        const enhancedSystemPrompt = renderSystemPrompt(profile);

        const aiReply = await generateAIResponse(conv.messages, enhancedSystemPrompt);
        conv.messages.push({ role: "assistant", content: aiReply });
        await dataAccess.saveSession(conv);

        res.json({ reply: aiReply });
      } catch (err) {
        console.error('fallback /chat/reply error', err);
        res.status(500).json({ error: 'Failed to generate reply' });
      }
    });
  }
})();

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/consent', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'consent.html'));
});

app.get('/participant-information', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'participant-information.html'));
});

app.get('/survey', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'survey.html'));
});

app.get('/debrief', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'debrief.html'));
});

app.get('/chat/:id', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'chat.html'));
});

app.get('/exit-survey', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'exit-survey.html'));
});

app.get('/cc-views-matrix', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'cc-views-matrix.html'));
});

app.get('/belief-confidence', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'belief-confidence.html'));
});

app.get('/intro_to_chatbot', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'intro_to_chatbot.html'));
});

app.get('/disqualified', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'disqualified.html'));
});

// Simple console logger for development endpoints
function logEndpoint(req, res, next) {
  if (process.env.NODE_ENV !== 'production') {
    const start = Date.now();
    res.on('finish', () => {
      const duration = Date.now() - start;
      console.log(`[${new Date().toISOString()}] ${req.method} ${req.path} - ${res.statusCode} (${duration}ms)`);
    });
  }
  next();
}

// Enhanced health check endpoint with database status
app.get('/health', logEndpoint, async (req, res) => {
  const startTime = Date.now();
  let dbAvailable = false;
  let dbConnectionTime = null;
  
  try {
    const dbStartTime = Date.now();
    dbAvailable = await database.isDatabaseAvailable();
    dbConnectionTime = Date.now() - dbStartTime;
  } catch (error) {
    console.error('Health check database error:', error.message);
  }
  
  const totalTime = Date.now() - startTime;
  
  res.json({
    ok: true,
    timestamp: new Date().toISOString(),
    database: {
      available: dbAvailable,
      connection_time_ms: dbConnectionTime
    },
    environment: process.env.NODE_ENV || 'development',
    response_time_ms: totalTime
  });
});

// Admin health check with database row counts (no auth required)
app.get('/api/admin/health-db', async (req, res) => {
    try {
        const dbAvailable = await database.isDatabaseAvailable();
        
        if (!dbAvailable) {
            return res.status(503).json({
                ok: false,
                database: {
                    available: false,
                    message: 'Database connection failed'
                },
                environment: process.env.NODE_ENV,
                timestamp: new Date().toISOString()
            });
        }
        
        // Get row counts
        const stats = await database.getDatabaseStats();
        
        res.json({
            ok: true,
            database: {
                available: true,
                connection_status: 'connected',
                row_counts: stats?.tables || {}
            },
            environment: process.env.NODE_ENV,
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        res.status(500).json({
            ok: false,
            error: error.message,
            environment: process.env.NODE_ENV,
            timestamp: new Date().toISOString()
        });
    }
});

// Legacy health endpoint (keeping for backwards compatibility)
app.get('/healthz', (req, res) => {
  res.json({
    ok: true,
    time: new Date().toISOString(),
    chatRouterMounted: Boolean(chatRouter),
    activeConversations: activeConversations.size
  });
});

// Database statistics endpoint
app.get('/api/database-stats', logEndpoint, async (req, res) => {
  try {
    const stats = await database.getDatabaseStats();
    
    if (!stats) {
      return res.status(503).json({
        error: 'Database statistics unavailable',
        message: 'Database connection not available'
      });
    }
    
    res.json({
      timestamp: new Date().toISOString(),
      ...stats
    });
    
  } catch (error) {
    console.error('Error getting database stats:', error);
    res.status(500).json({
      error: 'Failed to retrieve database statistics',
      message: error.message
    });
  }
});

// Debug endpoint for last session (no auth required)
app.get('/debug/last-session', logEndpoint, async (req, res) => {

  try {
    // Use data access layer which prefers Postgres over files
    const latestSession = await dataAccess.getLatestCompletedSession();
    
    if (!latestSession) {
      return res.json({ message: "No sessions found" });
    }

    res.json(latestSession);

  } catch (error) {
    console.error('Error fetching last session:', error);
    res.status(500).json({ error: 'Failed to fetch last session' });
  }
});

// Debug endpoint to check data files
app.get('/debug/data', (req, res) => {
    try {
        const participants = fs.existsSync(participantsDir) ? fs.readdirSync(participantsDir) : [];
        const conversations = fs.existsSync(conversationsDir) ? fs.readdirSync(conversationsDir) : [];
        const exports = fs.existsSync(exportsDir) ? fs.readdirSync(exportsDir) : [];
        
        res.json({
            dataFolderExists: fs.existsSync(dataDir),
            participantFiles: participants,
            conversationFiles: conversations,
            exportFiles: exports,
            totalFiles: participants.length + conversations.length + exports.length,
            directories: {
                dataDir,
                participantsDir,
                conversationsDir,
                exportsDir
            },
            activeConversations: Array.from(activeConversations.keys()),
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        res.json({ error: error.message });
    }
});

// Survey submission endpoint
app.post('/survey/submit', async (req, res) => {
    try {
        const {
            prolific_id,
            age,
            gender,
            education,
            // Political views
            economic_issues,
            social_issues,
            political_views_order,
            economic_issues_answered,
            social_issues_answered,
            // Belief change - legacy fields
            views_changed,
            viewsChanged, // Also accept legacy field name
            current_views,
            elaboration,
            ai_summary_generated,
            AI_Summary_Views,
            ai_accurate,
            confidence_level,
            missing_info,
            // New mind change variable (radio button system)
            mind_change_direction,
            mind_change_no_change,
            mind_change_other_text,
            consent,
            // Climate Change Skepticism (CCS) scale raw values
            ccs_01_raw, ccs_02_raw, ccs_03_raw, ccs_04_raw, ccs_05_raw, ccs_06_raw,
            ccs_07_raw, ccs_08_raw, ccs_09_raw, ccs_10_raw, ccs_11_raw, ccs_12_raw,
            // CCS scale scored values (with reverse coding applied)
            ccs_01_scored, ccs_02_scored, ccs_03_scored, ccs_04_scored, ccs_05_scored, ccs_06_scored,
            ccs_07_scored, ccs_08_scored, ccs_09_scored, ccs_10_scored, ccs_11_scored, ccs_12_scored,
            // CCS metadata (whether sliders were moved)
            ccs_01_was_moved, ccs_02_was_moved, ccs_03_was_moved, ccs_04_was_moved, ccs_05_was_moved, ccs_06_was_moved,
            ccs_07_was_moved, ccs_08_was_moved, ccs_09_was_moved, ccs_10_was_moved, ccs_11_was_moved, ccs_12_was_moved,
            // Attention check data
            attention_check_value,
            attention_check_passed,
            attention_check_was_moved,
            // CCS mean scores
            ccs_mean_scored,
            ccs_occurrence_mean,
            ccs_causation_mean,
            ccs_seriousness_mean,
            ccs_efficacy_mean,
            ccs_trust_mean,
            // Display order for analysis
            ccs_row_order,
            // Exit survey fields (may be passed through from frontend)
            summaryConfidence,
            finalConfidenceLevel,
            summaryAccurate
        } = req.body;
        
        console.log('Received survey submission:', req.body);
        console.log('DEBUG: Starting validation process...');
        
        // Check if we have the new mind change direction or legacy format
        const hasNewFormat = mind_change_direction !== undefined || mind_change_no_change !== undefined;
        
        if (hasNewFormat) {
            // New radio button system validation - a direction must be selected if "Yes" was chosen
            if (!mind_change_no_change && !mind_change_direction) {
                console.log('DEBUG: mind change validation FAILED - no direction selected');
                return res.status(400).json({ error: 'Please select the option that best describes how your views changed' });
            }
            
            // If "Other" is selected, validate that text is provided
            if (mind_change_direction === 'other' && (!mind_change_other_text || mind_change_other_text.trim() === '')) {
                console.log('DEBUG: other text validation FAILED - other selected but no text provided');
                return res.status(400).json({ error: 'Please describe your belief change when "Other" is selected' });
            }
            
            console.log('DEBUG: new mind change validation PASSED');
        } else {
            // Legacy validation for backwards compatibility
            const viewsChangedValue = views_changed || viewsChanged;
            if (!viewsChangedValue || !['Yes', 'No'].includes(viewsChangedValue)) {
                console.log('DEBUG: views_changed validation FAILED - views_changed:', viewsChangedValue);
                return res.status(400).json({ error: 'Please indicate whether your views on climate change have changed' });
            }
            console.log('DEBUG: legacy views_changed validation PASSED');
        }
        
        console.log('DEBUG: All validation passed, creating participant...');
        
        // Generate participant ID
        const participantId = `p_${uuidv4().replace(/-/g, '').substring(0, 6)}`;
        const now = new Date().toISOString();
        console.log('DEBUG: Generated participant ID:', participantId);
        
        // Create participant data with nested structure
        console.log('DEBUG: Creating participant data object...');
        const participantData = {
            // Top-level identification and metadata
            participant_id: participantId,
            prolific_id: prolific_id || null,
            consent: Boolean(consent),
            disqualified: false,
            timestamp_joined: now,
            
            // Demographics section
            demographics: {
                age: age ? parseInt(age) : null,
                gender: gender || null,
                education: education || null
            },
            
            // Belief change section
            belief_change: {
                // Legacy fields for backwards compatibility
                has_changed_mind: hasNewFormat ? (!mind_change_no_change && mind_change_direction) : ((views_changed || viewsChanged) === 'Yes'),
                current_view: current_views || null, // Raw text from participant
                elaboration: elaboration || null, // Raw text from participant
                ai_summary: ai_summary_generated || AI_Summary_Views || null, // AI summary of current_views + elaboration
                ai_confidence_slider: confidence_level !== undefined && confidence_level !== "N/a" ? parseInt(confidence_level) : null,
                ai_summary_accuracy: ai_accurate || summaryAccurate || null,
                chatbot_summary: null, // Will be populated after chatbot conversation
                
                // New mind change variables (radio button system)
                mind_change_direction: mind_change_direction || null,
                mind_change_no_change: mind_change_no_change === true || mind_change_no_change === 'on',
                mind_change_other_text: mind_change_direction === 'other' ? (mind_change_other_text || null) : null
            },
            
            // Views matrix section
            views_matrix: {
                climate_change_views: {
                    // CCS raw values
                    ccs_01_raw: ccs_01_raw || null,
                    ccs_02_raw: ccs_02_raw || null,
                    ccs_03_raw: ccs_03_raw || null,
                    ccs_04_raw: ccs_04_raw || null,
                    ccs_05_raw: ccs_05_raw || null,
                    ccs_06_raw: ccs_06_raw || null,
                    ccs_07_raw: ccs_07_raw || null,
                    ccs_08_raw: ccs_08_raw || null,
                    ccs_09_raw: ccs_09_raw || null,
                    ccs_10_raw: ccs_10_raw || null,
                    ccs_11_raw: ccs_11_raw || null,
                    ccs_12_raw: ccs_12_raw || null,
                    // CCS scored values
                    ccs_01_scored: ccs_01_scored || null,
                    ccs_02_scored: ccs_02_scored || null,
                    ccs_03_scored: ccs_03_scored || null,
                    ccs_04_scored: ccs_04_scored || null,
                    ccs_05_scored: ccs_05_scored || null,
                    ccs_06_scored: ccs_06_scored || null,
                    ccs_07_scored: ccs_07_scored || null,
                    ccs_08_scored: ccs_08_scored || null,
                    ccs_09_scored: ccs_09_scored || null,
                    ccs_10_scored: ccs_10_scored || null,
                    ccs_11_scored: ccs_11_scored || null,
                    ccs_12_scored: ccs_12_scored || null,
                    // CCS metadata
                    ccs_01_was_moved: ccs_01_was_moved || null,
                    ccs_02_was_moved: ccs_02_was_moved || null,
                    ccs_03_was_moved: ccs_03_was_moved || null,
                    ccs_04_was_moved: ccs_04_was_moved || null,
                    ccs_05_was_moved: ccs_05_was_moved || null,
                    ccs_06_was_moved: ccs_06_was_moved || null,
                    ccs_07_was_moved: ccs_07_was_moved || null,
                    ccs_08_was_moved: ccs_08_was_moved || null,
                    ccs_09_was_moved: ccs_09_was_moved || null,
                    ccs_10_was_moved: ccs_10_was_moved || null,
                    ccs_11_was_moved: ccs_11_was_moved || null,
                    ccs_12_was_moved: ccs_12_was_moved || null,
                    // Attention check
                    attention_check_value: attention_check_value || null,
                    attention_check_passed: attention_check_passed || null,
                    attention_check_was_moved: attention_check_was_moved || null,
                    // Mean scores
                    ccs_mean_scored: ccs_mean_scored || null,
                    ccs_occurrence_mean: ccs_occurrence_mean || null,
                    ccs_causation_mean: ccs_causation_mean || null,
                    ccs_seriousness_mean: ccs_seriousness_mean || null,
                    ccs_efficacy_mean: ccs_efficacy_mean || null,
                    ccs_trust_mean: ccs_trust_mean || null,
                    // Display order
                    ccs_row_order: ccs_row_order || null
                },
                political_views: {
                    economic_issues: economic_issues ? parseInt(economic_issues) : null,
                    social_issues: social_issues ? parseInt(social_issues) : null,
                    political_views_order: political_views_order || null,
                    economic_issues_answered: economic_issues_answered || null,
                    social_issues_answered: social_issues_answered || null
                }
            },
            
            // Chatbot interaction placeholder (will be populated later)
            chatbot_interaction: {
                messages: []
            },
            
            // Post-chat section (may be populated now or later)
            post_chat: {
                final_belief_confidence: finalConfidenceLevel !== undefined && finalConfidenceLevel !== "N/a" ? parseInt(finalConfidenceLevel) : null,
                chatbot_summary_accuracy: summaryConfidence ? parseInt(summaryConfidence) : null
            },
            
            // Timestamps section
            timestamps: {
                started: now,
                completed: null
            },
            
            // Legacy fields for backwards compatibility
            id: participantId, // Keep legacy id field
            createdAt: now,
            updatedAt: now
        };
        
        console.log('DEBUG: Participant data object created');
        
        // Save participant data to file system (always save regardless of eligibility)
        console.log('DEBUG: About to save participant data to files...');
        const filename = path.join(participantsDir, `${participantId}.json`);
        console.log('DEBUG: Filename:', filename);
        console.log('DEBUG: participantsDir exists:', fs.existsSync(participantsDir));
        
        // Save to file first (will be backup in dev, primary in dev without DB)
        if (!writeJson(filename, participantData)) {
            console.log('DEBUG: writeJson failed');
            throw new Error('Failed to save participant data to file');
        }
        
        console.log('DEBUG: writeJson succeeded');
        
        // Enhanced database save with better error handling and validation
        console.log('DEBUG: About to save participant data to database...');
        let databaseSaveResult = { success: false, error: null };
        
        try {
            const database = require('./database');
            const dbAvailable = await database.isDatabaseAvailable();
            
            if (dbAvailable) {
                console.log('💾 Database available - saving participant to PostgreSQL...');
                
                // Pre-save validation
                const validationErrors = [];
                if (!participantData.participant_id) {
                    validationErrors.push('Missing participant_id');
                }
                if (!participantData.prolific_id) {
                    validationErrors.push('Missing prolific_id');
                }
                if (!participantData.demographics) {
                    validationErrors.push('Missing demographics data');
                }
                if (!participantData.views_matrix?.climate_change_views) {
                    validationErrors.push('Missing climate change views data');
                }
                
                if (validationErrors.length > 0) {
                    console.warn('⚠️ Validation warnings for participant:', participantId, validationErrors);
                    // Don't fail - these are warnings only
                }
                
                // Log data structure for debugging
                console.log('📊 Survey data structure:', {
                    participant_id: participantData.participant_id,
                    has_demographics: !!participantData.demographics,
                    has_belief_change: !!participantData.belief_change,
                    has_views_matrix: !!participantData.views_matrix,
                    has_ccs_data: !!participantData.views_matrix?.climate_change_views,
                    has_political_data: !!participantData.views_matrix?.political_views,
                    ccs_mean_scored: participantData.views_matrix?.climate_change_views?.ccs_mean_scored,
                    economic_issues: participantData.views_matrix?.political_views?.economic_issues,
                    ai_summary: participantData.belief_change?.ai_summary?.substring(0, 50) + '...'
                });
                
                // Attempt database save with detailed error reporting
                const saveResult = await database.saveParticipant(participantData);
                if (saveResult) {
                    console.log('✅ Participant saved to database successfully:', participantId);
                    databaseSaveResult.success = true;
                } else {
                    throw new Error('Database save returned null - save failed');
                }
                
            } else {
                console.log('⚠️ Database unavailable - using file fallback only');
                databaseSaveResult.error = 'Database connection not available';
            }
        } catch (dbError) {
            console.error('❌ Database save failed for participant:', participantId, {
                error: dbError.message,
                stack: process.env.NODE_ENV === 'development' ? dbError.stack : undefined,
                participantData: {
                    participant_id: participantData.participant_id,
                    prolific_id: participantData.prolific_id,
                    has_demographics: !!participantData.demographics,
                    has_belief_change: !!participantData.belief_change,
                    has_views_matrix: !!participantData.views_matrix
                },
                timestamp: new Date().toISOString()
            });
            databaseSaveResult.error = dbError.message;
            // Continue - file save succeeded, don't fail the request
        }
        
        console.log('Participant saved successfully:', participantId, 'Database:', databaseSaveResult.success ? 'YES' : 'NO');
        console.log('DEBUG: Sending response with participantId:', participantId);
        
        res.json({
            participantId
        });
        
        console.log('DEBUG: Response sent successfully');
        
    } catch (error) {
        console.error('Error processing survey:', error);
        res.status(500).json({
            error: error.message || 'Internal server error'
        });
    }
});

// AI Summary generation endpoint
app.post('/api/generate-summary', async (req, res) => {
    try {
        const { text, currentViews, elaboration } = req.body;
        
        // Handle legacy single text field or new separate fields
        const inputCurrentViews = (currentViews || '').trim();
        const inputElaboration = (elaboration || '').trim();
        const combinedText = text || `${inputCurrentViews} ${inputElaboration}`.trim();
        
        // If both boxes are empty, do not generate a summary
        if (!combinedText) {
            return res.json({ summary: '' });
        }
        
        // If the participant only writes a very short answer, use that directly as the summary
        if (combinedText.length <= 50) {
            return res.json({ summary: combinedText });
        }
        
        console.log('Generating AI summary for text:', combinedText.substring(0, 100) + '...');
        
        // Check if OpenAI API key is configured
        if (!process.env.OPENAI_API_KEY) {
            console.log('OpenAI API key not configured. Using fallback summary.');
            const fallbackSummary = generateFallbackSummary(combinedText);
            return res.json({ summary: fallbackSummary });
        }
        
        try {
            // Prepare content description for the prompt
            let contentDescription = '';
            if (inputCurrentViews && inputElaboration) {
                contentDescription = `current views: "${inputCurrentViews}" and elaboration: "${inputElaboration}"`;
            } else if (inputCurrentViews) {
                contentDescription = `current views: "${inputCurrentViews}"`;
            } else if (inputElaboration) {
                contentDescription = `elaboration: "${inputElaboration}"`;
            } else {
                contentDescription = `text: "${combinedText}"`;
            }
            
            // Call OpenAI API to generate summary
            const completion = await openai.chat.completions.create({
                model: "gpt-4o-mini",
                messages: [
                    {
                        role: "system",
                        content: "Please provide a neutral summary of the views expressed about climate change. Write it as a direct statement of the views themselves, not as a description of what the participant thinks, feels, or believes. Avoid phrases like 'the participant feels,' 'the participant thinks,' 'the participant believes,' etc. Instead, phrase it as a direct statement of the views expressed. For example, instead of 'The participant feels frustrated that climate change is political,' write 'Climate change is being treated as a political issue.' Keep it accurate and concise. Write exactly one sentence with no preamble, bullet points, or quotation marks."
                    },
                    {
                        role: "user",
                        content: `${combinedText}`
                    }
                ],
                max_tokens: 100,
                temperature: 0.1
            });
            
            const summary = completion.choices[0]?.message?.content?.trim();
            
            if (!summary) {
                throw new Error('No summary received from OpenAI');
            }
            
            console.log('AI summary generated:', summary);
            res.json({ summary });
            
        } catch (error) {
            console.error('OpenAI API error:', error.message);
            // Use fallback summary if OpenAI fails
            const fallbackSummary = generateFallbackSummary(combinedText);
            console.log('Using fallback summary:', fallbackSummary);
            res.json({ summary: fallbackSummary });
        }
        
    } catch (error) {
        console.error('Error generating summary:', error);
        res.status(500).json({ error: error.message || 'Internal server error' });
    }
});

// Fallback summary generator
function generateFallbackSummary(text) {
    if (!text || text.trim().length === 0) {
        return "";
    }
    
    const trimmed = text.trim();
    
    // If short, return as-is
    if (trimmed.length <= 50) {
        return trimmed;
    }
    
    // For longer text, try to create a single sentence assertion
    // Remove common phrases that indicate uncertainty or hedging
    let cleaned = trimmed.replace(/^(I think|I believe|I feel|In my opinion|I guess|Maybe|Perhaps),?\s*/i, '');
    
    // Try to get the first complete sentence
    const sentences = cleaned.split(/[.!?]+/);
    if (sentences.length > 0 && sentences[0].trim().length > 10) {
        let firstSentence = sentences[0].trim();
        
        // Ensure it ends with a period
        if (!firstSentence.match(/[.!?]$/)) {
            firstSentence += '.';
        }
        
        return firstSentence;
    }
    
    // If no clear sentence structure, truncate at a reasonable length
    if (trimmed.length > 100) {
        return trimmed.substring(0, 97) + '...';
    }
    
    return trimmed;
}

// Start a new conversation
app.post('/api/conversations/start', async (req, res) => {
    const requestStart = Date.now();
    
    try {
        const { participantId } = req.body;
        
        if (!participantId) {
            return res.status(400).json({
                error: 'participantId is required',
                type: 'validation_error'
            });
        }
        
        // Check if participant exists
        const participantFile = path.join(participantsDir, `${participantId}.json`);
        const participant = readJson(participantFile);
        if (!participant) {
            return res.status(404).json({
                error: 'Participant not found',
                type: 'participant_not_found'
            });
        }
        
        // Generate conversation ID
        const conversationId = uuidv4();
        const now = new Date().toISOString();
        
        // System prompt for the conversation
        const systemPrompt = `You are an AI assistant facilitating a conversation about climate change. Your role is to engage thoughtfully and ask follow-up questions to help the participant explore their views. Do not try to persuade or change their mind - instead, focus on understanding their perspective and encouraging reflection. Keep responses conversational and under 150 words.`;
        
        // Create conversation data
        const conversationData = {
            id: conversationId,
            participantId: participantId,
            startedAt: now,
            endedAt: null,
            durationSeconds: null,
            systemPrompt: systemPrompt,
            messages: []
        };
        
        // Save conversation data using data access layer
        await dataAccess.saveSession(conversationData);
        
        // Track in memory
        activeConversations.set(conversationId, {
            participantId,
            startedAt: now,
            lastActivity: now
        });
        
        const requestDuration = Date.now() - requestStart;
        console.log(`Conversation started (${requestDuration}ms):`, conversationId, 'for participant:', participantId);
        
        res.json({ conversationId });
        
    } catch (error) {
        const requestDuration = Date.now() - requestStart;
        console.error(`Error starting conversation (${requestDuration}ms):`, error);
        
        res.status(500).json({
            error: 'Unable to start conversation. Please try again.',
            type: 'server_error',
            technical_error: process.env.NODE_ENV !== 'production' ? error.message : undefined
        });
    }
});

// Helper function to detect early-end phrase
function shouldEndNow(text) {
    if (!text) return false;
    // case-insensitive, allow surrounding punctuation/whitespace
    return /\bend the chat\b/i.test(text.trim());
}

// Send a message in a conversation
app.post('/api/conversations/:id/message', async (req, res) => {
    const requestStart = Date.now();
    
    try {
        const conversationId = req.params.id;
        const { content } = req.body;
        
        if (!content || !content.trim()) {
            return res.status(400).json({
                error: 'Message content is required',
                type: 'validation_error'
            });
        }
        
        // Check if conversation is active
        const activeConv = activeConversations.get(conversationId);
        if (!activeConv) {
            return res.status(404).json({
                error: 'Conversation not found or ended',
                type: 'conversation_not_found'
            });
        }
        
        // Check 5-minute time limit (300 seconds)
        const now = new Date();
        const startTime = new Date(activeConv.startedAt);
        const elapsedSeconds = Math.floor((now - startTime) / 1000);
        
        if (elapsedSeconds >= (CHAT_DURATION_MS / 1000)) {
            // End conversation due to time limit
            activeConversations.delete(conversationId);
            return res.status(410).json({
                error: 'Conversation time limit exceeded',
                type: 'timeout'
            });
        }
        
        // Check for early-end phrase
        if (shouldEndNow(content)) {
            // Load conversation data
            const filename = path.join(conversationsDir, `${conversationId}.json`);
            const conversationData = readJson(filename);
            if (!conversationData) {
                return res.status(404).json({
                    error: 'Conversation data not found',
                    type: 'data_error'
                });
            }
            
            // Add user message
            const userMessage = {
                role: 'user',
                content: content.trim(),
                timestamp: now.toISOString()
            };
            conversationData.messages.push(userMessage);
            
            // Ensure conversation has a summary before ending
            if (conversationData.participantId) {
                console.log('⚠️ Early chat end detected in main endpoint, ensuring summary exists');
                await fallbackEnsureConversationSummary(conversationId, conversationData.participantId);
                // Reload conversation data after potential summary addition
                const updatedConversation = readJson(filename);
                if (updatedConversation) {
                    conversationData.messages = updatedConversation.messages;
                }
            }
            
            // Add final assistant message
            const finalMessage = {
                role: 'assistant',
                content: 'Okay — ending the chat now. Thanks for participating!',
                timestamp: new Date().toISOString()
            };
            conversationData.messages.push(finalMessage);
            
            // End conversation
            const durationSeconds = Math.floor((now - startTime) / 1000);
            conversationData.endedAt = now.toISOString();
            conversationData.durationSeconds = durationSeconds;
            
            // Save conversation using data access layer
            await dataAccess.saveSession(conversationData);
            
            // Update participant data with conversation messages
            try {
                const participantFile = path.join(participantsDir, `${conversationData.participantId}.json`);
                const participantData = readJson(participantFile);
                
                if (participantData) {
                    // Transform conversation messages to match desired structure
                    const transformedMessages = conversationData.messages.map(msg => ({
                        sender: msg.role === 'user' ? 'participant' : 'chatbot',
                        text: msg.content,
                        timestamp: msg.timestamp || now.toISOString()
                    }));
                    
                    // Update chatbot interaction section
                    participantData.chatbot_interaction = {
                        messages: transformedMessages
                    };
                    
                    // Generate chatbot summary from conversation messages and add to belief_change
                    if (transformedMessages.length > 0) {
                        const conversationText = transformedMessages
                            .filter(msg => msg.sender === 'participant')
                            .map(msg => msg.text)
                            .join(' ');
                        
                        if (conversationText.trim()) {
                            participantData.belief_change.chatbot_summary = `Participant discussed: ${conversationText.substring(0, 200)}${conversationText.length > 200 ? '...' : ''}`;
                        }
                    }
                    
                    // Update timestamp
                    participantData.updatedAt = now.toISOString();
                    
                    // Save updated participant data
                    await database.saveParticipant(participantData);
                    console.log(`Updated participant ${conversationData.participantId} with ${transformedMessages.length} conversation messages (early end)`);
                }
            } catch (error) {
                console.error('Error updating participant with early-end conversation data:', error);
                // Don't fail the endpoint if participant update fails
            }
            
            // Remove from active conversations
            activeConversations.delete(conversationId);
            
            console.log('Conversation ended by phrase:', conversationId, 'Duration:', durationSeconds, 'seconds');
            
            return res.json({
                reply: finalMessage.content,
                sessionEnded: true
            });
        }
        
        // Load conversation data
        const filename = path.join(conversationsDir, `${conversationId}.json`);
        const conversationData = readJson(filename);
        if (!conversationData) {
            return res.status(404).json({
                error: 'Conversation data not found',
                type: 'data_error'
            });
        }
        
        // Add user message
        const userMessage = {
            role: 'user',
            content: content.trim(),
            timestamp: now.toISOString()
        };
        conversationData.messages.push(userMessage);
        
        // Call OpenAI API with timeout handling
        const assistantReply = await generateAIResponse(conversationData.messages, conversationData.systemPrompt);
        
        // Add assistant message
        const assistantMessage = {
            role: 'assistant',
            content: assistantReply,
            timestamp: new Date().toISOString()
        };
        conversationData.messages.push(assistantMessage);
        
        // Update last activity
        activeConv.lastActivity = now.toISOString();
        
        // Save updated conversation using data access layer
        await dataAccess.saveSession(conversationData);
        
        const requestDuration = Date.now() - requestStart;
        console.log(`Chat message processed in ${requestDuration}ms`);
        
        res.json({ reply: assistantReply });
        
    } catch (error) {
        const requestDuration = Date.now() - requestStart;
        console.error(`Error processing message (${requestDuration}ms):`, error);
        
        // Provide user-friendly error messages based on error type
        let userMessage = 'We encountered a technical issue. Please try sending your message again.';
        let errorType = 'server_error';
        
        if (error.message.includes('timeout') || error.message.includes('ECONNRESET')) {
            userMessage = 'The connection timed out. Please try sending your message again.';
            errorType = 'connection_timeout';
        } else if (error.message.includes('ENOTFOUND') || error.message.includes('network')) {
            userMessage = 'Unable to connect to our services. Please check your connection and try again.';
            errorType = 'network_error';
        } else if (error.message.includes('rate limit')) {
            userMessage = 'Too many requests. Please wait a moment and try again.';
            errorType = 'rate_limit';
        }
        
        res.status(500).json({
            error: userMessage,
            type: errorType,
            technical_error: process.env.NODE_ENV !== 'production' ? error.message : undefined
        });
    }
});

// End a conversation
app.post('/api/conversations/:id/end', async (req, res) => {
    const requestStart = Date.now();
    
    try {
        const conversationId = req.params.id;
        
        // Check if conversation is active
        const activeConv = activeConversations.get(conversationId);
        if (!activeConv) {
            return res.status(404).json({
                error: 'Conversation not found or already ended',
                type: 'conversation_not_found'
            });
        }
        
        // Load conversation data
        const filename = path.join(conversationsDir, `${conversationId}.json`);
        const conversationData = readJson(filename);
        if (!conversationData) {
            return res.status(404).json({
                error: 'Conversation data not found',
                type: 'data_error'
            });
        }
        
        // Calculate duration
        const now = new Date();
        const startTime = new Date(conversationData.startedAt);
        const durationSeconds = Math.floor((now - startTime) / 1000);
        
        // Ensure conversation has a summary before ending (time-based ending)
        if (conversationData.participantId) {
            console.log('⏰ Time-based conversation end detected, ensuring summary exists');
            await fallbackEnsureConversationSummary(conversationId, conversationData.participantId);
            // Reload conversation data after potential summary addition
            const filename = path.join(conversationsDir, `${conversationId}.json`);
            const updatedConversation = readJson(filename);
            if (updatedConversation) {
                conversationData.messages = updatedConversation.messages;
            }
        }
        
        // Update conversation data
        conversationData.endedAt = now.toISOString();
        conversationData.durationSeconds = durationSeconds;
        
        // Save updated conversation using data access layer
        await dataAccess.saveSession(conversationData);
        
        // Update participant data with conversation messages
        try {
            const participantFile = path.join(participantsDir, `${conversationData.participantId}.json`);
            const participantData = readJson(participantFile);
            
            if (participantData) {
                // Transform conversation messages to match desired structure
                const transformedMessages = conversationData.messages.map(msg => ({
                    sender: msg.role === 'user' ? 'participant' : 'chatbot',
                    text: msg.content,
                    timestamp: msg.timestamp || now.toISOString()
                }));
                
                // Update chatbot interaction section
                participantData.chatbot_interaction = {
                    messages: transformedMessages
                };
                
                // Generate chatbot summary from conversation messages and add to belief_change
                if (transformedMessages.length > 0) {
                    const conversationText = transformedMessages
                        .filter(msg => msg.sender === 'participant')
                        .map(msg => msg.text)
                        .join(' ');
                    
                    if (conversationText.trim()) {
                        participantData.belief_change.chatbot_summary = `Participant discussed: ${conversationText.substring(0, 200)}${conversationText.length > 200 ? '...' : ''}`;
                    }
                }
                
                // Update timestamp
                participantData.updatedAt = now.toISOString();
                
                // Save updated participant data
                await database.saveParticipant(participantData);
                console.log(`Updated participant ${conversationData.participantId} with ${transformedMessages.length} conversation messages`);
            }
        } catch (error) {
            console.error('Error updating participant with conversation data:', error);
            // Don't fail the endpoint if participant update fails
        }
        
        // Remove from active conversations
        activeConversations.delete(conversationId);
        
        const requestDuration = Date.now() - requestStart;
        console.log(`Conversation ended (${requestDuration}ms):`, conversationId, 'Duration:', durationSeconds, 'seconds');
        
        res.json({ ok: true });
        
    } catch (error) {
        const requestDuration = Date.now() - requestStart;
        console.error(`Error ending conversation (${requestDuration}ms):`, error);
        
        res.status(500).json({
            error: 'Unable to end conversation properly. Your data has been saved.',
            type: 'server_error',
            technical_error: process.env.NODE_ENV !== 'production' ? error.message : undefined
        });
    }
});

// Get participant data endpoint
app.get('/api/participant/:id', (req, res) => {
    try {
        const participantId = req.params.id;
        
        // Read participant file
        const filename = path.join(participantsDir, `${participantId}.json`);
        const participantData = readJson(filename);
        
        if (!participantData) {
            return res.status(404).json({ error: 'Participant not found' });
        }
        
        res.json(participantData);
        
    } catch (error) {
        console.error('Error getting participant data:', error);
        res.status(500).json({ error: error.message || 'Internal server error' });
    }
});

// Honeypot submission endpoint - saves bot responses and redirects
app.post('/api/honeypot-submission', async (req, res) => {
    try {
        const {
            current_views,
            elaboration,
            participant_session_data
        } = req.body;
        
        console.log('Honeypot triggered - saving bot response');
        
        // Generate participant ID for the bot response
        const participantId = uuidv4();
        const now = new Date().toISOString();
        
        // Parse session data if available
        let sessionData = {};
        try {
            sessionData = participant_session_data ? JSON.parse(participant_session_data) : {};
        } catch (e) {
            console.warn('Failed to parse session data:', e);
        }
        
        // Create participant data with honeypot flags
        const participantData = {
            id: participantId,
            createdAt: now,
            updatedAt: now,
            
            // Mark as honeypot triggered
            honeypot_triggered: true,
            disqualification_reason: "automated entry detected",
            
            // Store the bot response
            current_views: current_views || null,
            elaboration: elaboration || null,
            
            // Include any session data that was available
            ...sessionData,
            
            // Additional metadata
            bot_detection_method: "hello_repetition",
            user_agent: req.headers['user-agent'] || null,
            ip_address: req.ip || null
        };
        
        // Save the bot response for review
        const result = await database.saveParticipant(participantData);
        if (!result) {
            throw new Error('Failed to save honeypot data');
        }
        
        console.log('Bot response saved with ID:', participantId);
        
        // Return success so client can handle redirect
        res.json({
            success: true,
            redirect_url: 'https://app.prolific.com/submissions/complete?cc=SCREENOUT'
        });
        
    } catch (error) {
        console.error('Error processing honeypot submission:', error);
        res.status(500).json({
            error: 'Processing error',
            redirect_url: 'https://app.prolific.com/submissions/complete?cc=SCREENOUT'
        });
    }
});

// End survey submission endpoint
app.post('/api/end-survey', async (req, res) => {
    try {
        const {
            participant_id,
            summaryConfidence,
            finalConfidenceLevel
        } = req.body;
        
        console.log('Received end survey submission:', req.body);
        
        // Validate required fields
        if (!participant_id) {
            return res.status(400).json({ error: 'Participant ID is required' });
        }
        
        // Load existing participant data
        const participantFile = path.join(participantsDir, `${participant_id}.json`);
        const participantData = readJson(participantFile);
        
        if (!participantData) {
            return res.status(404).json({ error: 'Participant not found' });
        }
        
        const now = new Date().toISOString();
        
        // Update post_chat section with exit survey data
        participantData.post_chat = {
            final_belief_confidence: finalConfidenceLevel !== undefined && finalConfidenceLevel !== "N/a" ? parseInt(finalConfidenceLevel) : null,
            chatbot_summary_accuracy: summaryConfidence ? parseInt(summaryConfidence) : null
        };
        
        // Update completion timestamp
        participantData.timestamps.completed = now;
        participantData.updatedAt = now;
        
        // Save updated participant data
        const result = await database.saveParticipant(participantData);
        if (!result) {
            throw new Error('Failed to update participant data');
        }
        
        // Save updated participant data to database
        try {
            const database = require('./database');
            const dbAvailable = await database.isDatabaseAvailable();
            
            if (dbAvailable) {
                console.log('💾 Updating participant in database for end survey:', participant_id);
                await database.saveParticipant(participantData);
                console.log('✅ End survey data saved to database successfully');
            } else {
                console.log('⚠️ Database unavailable for end survey update - file saved only');
            }
        } catch (dbError) {
            console.error('❌ Database update failed for end survey:', participant_id, dbError.message);
            // Continue - file save succeeded, don't fail the request
        }
        
        console.log('End survey data integrated successfully for participant:', participant_id);
        
        res.json({ ok: true, participant_id: participant_id });
        
    } catch (error) {
        console.error('Error processing end survey:', error);
        res.status(500).json({
            error: error.message || 'Internal server error'
        });
    }
});

// Chatbot summary validation endpoint
app.post('/api/chatbot-summary-validation', async (req, res) => {
    try {
        const {
            participant_id,
            summary_accurate,
            chatbot_summary,
            timestamp
        } = req.body;
        
        console.log('Received chatbot summary validation:', req.body);
        
        // Validate required fields
        if (!participant_id) {
            return res.status(400).json({ error: 'Participant ID is required' });
        }
        
        if (!summary_accurate || !['Yes', 'No'].includes(summary_accurate)) {
            return res.status(400).json({ error: 'Valid summary accuracy response is required' });
        }
        
        // Load existing participant data
        const participantFile = path.join(participantsDir, `${participant_id}.json`);
        const participantData = readJson(participantFile);
        
        if (!participantData) {
            return res.status(404).json({ error: 'Participant not found' });
        }
        
        const now = new Date().toISOString();
        
        // Update belief_change section with chatbot summary validation
        if (!participantData.belief_change) {
            participantData.belief_change = {};
        }
        
        // Save the chatbot summary validation response
        participantData.belief_change.chatbot_summary_validation = summary_accurate;
        
        // Also update post_chat section for consistency
        if (!participantData.post_chat) {
            participantData.post_chat = {};
        }
        participantData.post_chat.chatbot_summary_accuracy = summary_accurate;
        
        // Store the chatbot summary if provided
        if (chatbot_summary) {
            participantData.belief_change.chatbot_summary_bullets = chatbot_summary;
        }
        
        // Update timestamp
        participantData.updatedAt = now;
        
        // Save updated participant data
        const result = await database.saveParticipant(participantData);
        if (!result) {
            throw new Error('Failed to update participant data');
        }
        
        // Save updated participant data to database
        try {
            const database = require('./database');
            const dbAvailable = await database.isDatabaseAvailable();
            
            if (dbAvailable) {
                console.log('💾 Updating participant in database for chatbot summary validation:', participant_id);
                await database.saveParticipant(participantData);
                console.log('✅ Chatbot summary validation data saved to database successfully');
            } else {
                console.log('⚠️ Database unavailable for chatbot summary validation update - file saved only');
            }
        } catch (dbError) {
            console.error('❌ Database update failed for chatbot summary validation:', participant_id, dbError.message);
            // Continue - file save succeeded, don't fail the request
        }
        
        console.log('Chatbot summary validation saved successfully for participant:', participant_id);
        
        res.json({ ok: true, participant_id: participant_id });
        
    } catch (error) {
        console.error('Error processing chatbot summary validation:', error);
        res.status(500).json({
            error: error.message || 'Internal server error'
        });
    }
});

// Database export endpoint using raw PostgreSQL (no auth required)
app.get('/export/database', async (req, res) => {
  try {
    const startTime = Date.now();
    
    // Check if database is available
    const dbAvailable = await database.isDatabaseAvailable();
    if (!dbAvailable) {
      return res.status(503).json({
        error: 'Database unavailable',
        message: 'Database connection not available. Try /api/admin/export.json for file-based export.'
      });
    }
    
    console.log('🗃️ Starting database export...');
    
    // Get all data using new database functions
    const [participants, sessions, messages] = await Promise.all([
      database.getAllParticipants(),
      database.getAllSessions(),
      database.getAllMessages()
    ]);
    
    // Calculate completed surveys
    const completedSurveys = participants.filter(p =>
      p.timestamps && p.timestamps.completed ||
      p.post_chat && (p.post_chat.final_belief_confidence !== null || p.post_chat.chatbot_summary_accuracy !== null)
    ).length;
    
    // Create export structure
    const exportData = {
      exported_at: new Date().toISOString(),
      export_method: 'database',
      totals: {
        participants: participants.length,
        sessions: sessions.length,
        messages: messages.length,
        completed_surveys: completedSurveys
      },
      data: {
        participants: participants,
        sessions: sessions,
        messages: messages
      },
      export_duration_ms: Date.now() - startTime
    };
    
    // Set headers for file download
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="data-export-${new Date().toISOString().split('T')[0]}.json"`);
    
    const exportDuration = Date.now() - startTime;
    console.log(`✅ Database export completed in ${exportDuration}ms: ${participants.length} participants, ${sessions.length} sessions, ${messages.length} messages`);
    
    res.json(exportData);
    
  } catch (error) {
    console.error('❌ Database export failed:', error);
    res.status(500).json({
      error: 'Database export failed',
      message: error.message,
      fallback_available: true
    });
  }
});

// Admin export endpoints
function requireAdmin(req, res, next) {
  const token = req.headers['x-admin-token'];
  if (!process.env.ADMIN_TOKEN) {
    console.warn('ADMIN_TOKEN not configured; blocking admin endpoints by default.');
    return res.status(403).json({ error: 'Admin token not configured' });
  }
  if (token !== process.env.ADMIN_TOKEN) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  next();
}

// Export consolidated JSON data (new format - no auth required)
app.get('/api/admin/export.json', async (req, res) => {
    try {
        // Use database functions instead of reading files directly
        const participants = await database.getAllParticipants();
        const conversations = await database.getAllSessions();
        const messagesData = await database.getAllMessages();
        
        let totalMessages = 0;
        
        // Process participants to ensure complete structure
        const processedParticipants = participants.map(participant => {
            if (participant) {
                // Ensure participant has complete structure and all variables are captured
                const completeParticipant = {
                            // Core identification
                            participant_id: participant.participant_id,
                            prolific_id: participant.prolific_id,
                            consent: participant.consent,
                            disqualified: participant.disqualified || false,
                            timestamp_joined: participant.timestamp_joined,
                            
                            // Demographics - ensure all fields are captured
                            demographics: {
                                age: participant.demographics?.age || null,
                                gender: participant.demographics?.gender || null,
                                education: participant.demographics?.education || null
                            },
                            
                            // Belief change - ensure all fields are captured
                            belief_change: {
                                has_changed_mind: participant.belief_change?.has_changed_mind || false,
                                current_view: participant.belief_change?.current_view || null,
                                elaboration: participant.belief_change?.elaboration || null,
                                ai_summary: participant.belief_change?.ai_summary || null,
                                ai_confidence_slider: participant.belief_change?.ai_confidence_slider || null,
                                ai_summary_accuracy: participant.belief_change?.ai_summary_accuracy || null,
                                chatbot_summary: participant.belief_change?.chatbot_summary || null,
                                chatbot_summary_validation: participant.belief_change?.chatbot_summary_validation || null,
                                chatbot_summary_bullets: participant.belief_change?.chatbot_summary_bullets || null,
                                // New mind change variables (radio button system)
                                mind_change_direction: participant.belief_change?.mind_change_direction || null,
                                mind_change_no_change: participant.belief_change?.mind_change_no_change || false,
                                mind_change_other_text: participant.belief_change?.mind_change_other_text || null,
                                // Legacy mind change variables for backwards compatibility
                                mind_change_real_to_not_real: participant.belief_change?.mind_change_real_to_not_real || false,
                                mind_change_not_urgent_to_serious_crisis: participant.belief_change?.mind_change_not_urgent_to_serious_crisis || false,
                                mind_change_not_real_to_real: participant.belief_change?.mind_change_not_real_to_real || false,
                                mind_change_natural_to_human_caused: participant.belief_change?.mind_change_natural_to_human_caused || false,
                                mind_change_human_caused_to_natural: participant.belief_change?.mind_change_human_caused_to_natural || false,
                                mind_change_serious_crisis_to_not_urgent: participant.belief_change?.mind_change_serious_crisis_to_not_urgent || false,
                                mind_change_other_selected: participant.belief_change?.mind_change_other_selected || false
                            },
                            
                            // Views matrix - ensure ALL climate change scale variables are captured
                            views_matrix: {
                                climate_change_views: {
                                    // CCS raw values (ccs_01_raw through ccs_12_raw)
                                    ccs_01_raw: participant.views_matrix?.climate_change_views?.ccs_01_raw || null,
                                    ccs_02_raw: participant.views_matrix?.climate_change_views?.ccs_02_raw || null,
                                    ccs_03_raw: participant.views_matrix?.climate_change_views?.ccs_03_raw || null,
                                    ccs_04_raw: participant.views_matrix?.climate_change_views?.ccs_04_raw || null,
                                    ccs_05_raw: participant.views_matrix?.climate_change_views?.ccs_05_raw || null,
                                    ccs_06_raw: participant.views_matrix?.climate_change_views?.ccs_06_raw || null,
                                    ccs_07_raw: participant.views_matrix?.climate_change_views?.ccs_07_raw || null,
                                    ccs_08_raw: participant.views_matrix?.climate_change_views?.ccs_08_raw || null,
                                    ccs_09_raw: participant.views_matrix?.climate_change_views?.ccs_09_raw || null,
                                    ccs_10_raw: participant.views_matrix?.climate_change_views?.ccs_10_raw || null,
                                    ccs_11_raw: participant.views_matrix?.climate_change_views?.ccs_11_raw || null,
                                    ccs_12_raw: participant.views_matrix?.climate_change_views?.ccs_12_raw || null,
                                    // CCS scored values (ccs_01_scored through ccs_12_scored)
                                    ccs_01_scored: participant.views_matrix?.climate_change_views?.ccs_01_scored || null,
                                    ccs_02_scored: participant.views_matrix?.climate_change_views?.ccs_02_scored || null,
                                    ccs_03_scored: participant.views_matrix?.climate_change_views?.ccs_03_scored || null,
                                    ccs_04_scored: participant.views_matrix?.climate_change_views?.ccs_04_scored || null,
                                    ccs_05_scored: participant.views_matrix?.climate_change_views?.ccs_05_scored || null,
                                    ccs_06_scored: participant.views_matrix?.climate_change_views?.ccs_06_scored || null,
                                    ccs_07_scored: participant.views_matrix?.climate_change_views?.ccs_07_scored || null,
                                    ccs_08_scored: participant.views_matrix?.climate_change_views?.ccs_08_scored || null,
                                    ccs_09_scored: participant.views_matrix?.climate_change_views?.ccs_09_scored || null,
                                    ccs_10_scored: participant.views_matrix?.climate_change_views?.ccs_10_scored || null,
                                    ccs_11_scored: participant.views_matrix?.climate_change_views?.ccs_11_scored || null,
                                    ccs_12_scored: participant.views_matrix?.climate_change_views?.ccs_12_scored || null,
                                    // CCS metadata (was_moved flags)
                                    ccs_01_was_moved: participant.views_matrix?.climate_change_views?.ccs_01_was_moved || null,
                                    ccs_02_was_moved: participant.views_matrix?.climate_change_views?.ccs_02_was_moved || null,
                                    ccs_03_was_moved: participant.views_matrix?.climate_change_views?.ccs_03_was_moved || null,
                                    ccs_04_was_moved: participant.views_matrix?.climate_change_views?.ccs_04_was_moved || null,
                                    ccs_05_was_moved: participant.views_matrix?.climate_change_views?.ccs_05_was_moved || null,
                                    ccs_06_was_moved: participant.views_matrix?.climate_change_views?.ccs_06_was_moved || null,
                                    ccs_07_was_moved: participant.views_matrix?.climate_change_views?.ccs_07_was_moved || null,
                                    ccs_08_was_moved: participant.views_matrix?.climate_change_views?.ccs_08_was_moved || null,
                                    ccs_09_was_moved: participant.views_matrix?.climate_change_views?.ccs_09_was_moved || null,
                                    ccs_10_was_moved: participant.views_matrix?.climate_change_views?.ccs_10_was_moved || null,
                                    ccs_11_was_moved: participant.views_matrix?.climate_change_views?.ccs_11_was_moved || null,
                                    ccs_12_was_moved: participant.views_matrix?.climate_change_views?.ccs_12_was_moved || null,
                                    // Attention check
                                    attention_check_value: participant.views_matrix?.climate_change_views?.attention_check_value || null,
                                    attention_check_passed: participant.views_matrix?.climate_change_views?.attention_check_passed || null,
                                    attention_check_was_moved: participant.views_matrix?.climate_change_views?.attention_check_was_moved || null,
                                    // Mean scores
                                    ccs_mean_scored: participant.views_matrix?.climate_change_views?.ccs_mean_scored || null,
                                    ccs_occurrence_mean: participant.views_matrix?.climate_change_views?.ccs_occurrence_mean || null,
                                    ccs_causation_mean: participant.views_matrix?.climate_change_views?.ccs_causation_mean || null,
                                    ccs_seriousness_mean: participant.views_matrix?.climate_change_views?.ccs_seriousness_mean || null,
                                    ccs_efficacy_mean: participant.views_matrix?.climate_change_views?.ccs_efficacy_mean || null,
                                    ccs_trust_mean: participant.views_matrix?.climate_change_views?.ccs_trust_mean || null,
                                    // Display order
                                    ccs_row_order: participant.views_matrix?.climate_change_views?.ccs_row_order || null
                                },
                                political_views: {
                                    economic_issues: participant.views_matrix?.political_views?.economic_issues || null,
                                    social_issues: participant.views_matrix?.political_views?.social_issues || null,
                                    political_views_order: participant.views_matrix?.political_views?.political_views_order || null,
                                    economic_issues_answered: participant.views_matrix?.political_views?.economic_issues_answered || null,
                                    social_issues_answered: participant.views_matrix?.political_views?.social_issues_answered || null
                                }
                            },
                            
                            // Chatbot interaction - ensure messages are captured
                            chatbot_interaction: {
                                messages: participant.chatbot_interaction?.messages || []
                            },
                            
                            // Post chat data
                            post_chat: {
                                final_belief_confidence: participant.post_chat?.final_belief_confidence || null,
                                chatbot_summary_accuracy: participant.post_chat?.chatbot_summary_accuracy || null
                            },
                            
                            // Timestamps
                            timestamps: {
                                started: participant.timestamps?.started || participant.timestamp_joined,
                                completed: participant.timestamps?.completed || null
                            },
                            
                            // Legacy fields for backwards compatibility
                            id: participant.id || participant.participant_id,
                    createdAt: participant.createdAt || participant.timestamp_joined,
                    updatedAt: participant.updatedAt || participant.timestamp_joined
                };
                
                // Count messages from participant chatbot interactions
                if (completeParticipant.chatbot_interaction && completeParticipant.chatbot_interaction.messages) {
                    totalMessages += completeParticipant.chatbot_interaction.messages.length;
                }
                
                return completeParticipant;
            }
            return null;
        }).filter(p => p);
        
        // Create consolidated export structure
        const exportData = {
            exported_at: new Date().toISOString(),
            totals: {
                participants: processedParticipants.length,
                conversations: conversations.length,
                messages: messagesData.length,
                completed_surveys: processedParticipants.filter(p => p.timestamps && p.timestamps.completed).length
            },
            data: {
                participants: processedParticipants,
                conversations: conversations,
                messages: messagesData
            }
        };
        
        res.json(exportData);
        
    } catch (error) {
        console.error('Error exporting consolidated JSON data:', error);
        res.status(500).json({ error: error.message || 'Internal server error' });
    }
});

// Export legacy JSON format (individual arrays)
app.get('/api/admin/export-legacy.json', requireAdmin, (req, res) => {
    try {
        const participants = [];
        const conversations = [];
        
        // Read all participant files
        if (fs.existsSync(participantsDir)) {
            const participantFiles = fs.readdirSync(participantsDir);
            for (const file of participantFiles) {
                if (file.endsWith('.json')) {
                    const participant = readJson(path.join(participantsDir, file));
                    if (participant) {
                        participants.push(participant);
                    }
                }
            }
        }
        
        // Read all conversation files
        if (fs.existsSync(conversationsDir)) {
            const conversationFiles = fs.readdirSync(conversationsDir);
            for (const file of conversationFiles) {
                if (file.endsWith('.json')) {
                    const conversation = readJson(path.join(conversationsDir, file));
                    if (conversation) {
                        conversations.push(conversation);
                    }
                }
            }
        }
        
        res.json({ participants, conversations });
        
    } catch (error) {
        console.error('Error exporting legacy JSON data:', error);
        res.status(500).json({ error: error.message || 'Internal server error' });
    }
});

// Generate and serve consolidated export file
app.get('/api/admin/generate-export', requireAdmin, (req, res) => {
    try {
        const { generateConsolidatedExport } = require('./generate_export.js');
        const exportPath = generateConsolidatedExport();
        
        if (exportPath && fs.existsSync(exportPath)) {
            console.log('📤 Serving generated export file:', exportPath);
            res.download(exportPath, `research-data-export-${new Date().toISOString().split('T')[0]}.json`);
        } else {
            throw new Error('Failed to generate export file');
        }
        
    } catch (error) {
        console.error('Error generating export file:', error);
        res.status(500).json({ error: error.message || 'Internal server error' });
    }
});

// Export CSV data
app.get('/api/admin/export.csv', requireAdmin, (req, res) => {
    try {
        // Create participants lookup
        const participantLookup = new Map();
        if (fs.existsSync(participantsDir)) {
            const participantFiles = fs.readdirSync(participantsDir);
            for (const file of participantFiles) {
                if (file.endsWith('.json')) {
                    const participant = readJson(path.join(participantsDir, file));
                    if (participant) {
                        participantLookup.set(participant.id, participant);
                    }
                }
            }
        }
        
        // CSV header
        const csvRows = [
            'participantId,conversationId,timestamp,role,content,age,gender,country,education,politicalOrientation,priorBelief,currentBelief'
        ];
        
        // Process conversations
        if (fs.existsSync(conversationsDir)) {
            const conversationFiles = fs.readdirSync(conversationsDir);
            for (const file of conversationFiles) {
                if (file.endsWith('.json')) {
                    const conversation = readJson(path.join(conversationsDir, file));
                    if (conversation && conversation.messages) {
                        const participant = participantLookup.get(conversation.participantId);
                        
                        for (const message of conversation.messages) {
                            const row = [
                                escapeCsv(conversation.participantId || ''),
                                escapeCsv(conversation.id || ''),
                                escapeCsv(message.timestamp || ''),
                                escapeCsv(message.role || ''),
                                escapeCsv(message.content || ''),
                                participant ? (participant.age || '') : '',
                                participant ? escapeCsv(participant.gender || '') : '',
                                participant ? escapeCsv(participant.country || '') : '',
                                participant ? escapeCsv(participant.education || '') : '',
                                participant ? escapeCsv(participant.politicalOrientation || '') : '',
                                participant ? escapeCsv(participant.priorBelief || '') : '',
                                participant ? escapeCsv(participant.currentBelief || '') : ''
                            ].join(',');
                            
                            csvRows.push(row);
                        }
                    }
                }
            }
        }
        
        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', 'attachment; filename="conversation_export.csv"');
        res.send(csvRows.join('\n'));
        
    } catch (error) {
        console.error('Error exporting CSV data:', error);
        res.status(500).json({ error: error.message || 'Internal server error' });
    }
});

// Clear all data endpoint (DANGEROUS - no auth required)
app.delete('/api/admin/clear-all-data', async (req, res) => {
    try {
        // Additional safety check - require confirmation parameter
        const { confirm } = req.query;
        if (confirm !== 'DELETE_ALL_DATA') {
            return res.status(400).json({
                error: 'Missing confirmation parameter',
                required: 'Add ?confirm=DELETE_ALL_DATA to the URL to confirm data deletion',
                warning: 'This will permanently delete ALL research data from both database and files'
            });
        }

        console.log('🚨 ADMIN DATA CLEAR INITIATED - All research data will be deleted');
        
        // Import the clear function from database
        const database = require('./database');
        const result = await database.clearAllData();
        
        if (result.success) {
            console.log('🗑️ All data successfully cleared by admin');
            res.json({
                success: true,
                message: 'All research data has been permanently deleted',
                ...result
            });
        } else {
            console.error('❌ Data clear operation failed:', result.error);
            res.status(500).json({
                success: false,
                error: 'Data clear operation failed',
                details: result
            });
        }
        
    } catch (error) {
        console.error('❌ Clear endpoint error:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to clear data',
            message: error.message
        });
    }
});

function escapeCsv(str) {
    if (typeof str !== 'string') return str;
    if (str.includes(',') || str.includes('"') || str.includes('\n')) {
        return '"' + str.replace(/"/g, '""') + '"';
    }
    return str;
}

// Summary generation functions for safety net (fallback endpoints)
function fallbackHasExistingSummary(messages) {
  // Check if conversation already has a structured summary from the chatbot
  if (!messages || messages.length === 0) return false;
  
  // Look for assistant messages that contain bullet points or summary indicators
  const assistantMessages = messages.filter(msg => msg.role === 'assistant');
  
  for (const msg of assistantMessages.slice(-5)) { // Check last 5 assistant messages
    const content = msg.content || '';
    
    // Check for bullet point patterns
    if (content.includes('•') || content.includes('*') || content.includes('-')) {
      // Check if it has summary-like structure (multiple points)
      const bulletMatches = content.match(/[•\*\-]/g);
      if (bulletMatches && bulletMatches.length >= 2) {
        console.log('✓ Found existing fallback summary with bullet points');
        return true;
      }
    }
    
    // Check for summary keywords
    if (content.toLowerCase().includes('summarize') ||
        content.toLowerCase().includes('summary') ||
        content.toLowerCase().includes('key themes') ||
        content.toLowerCase().includes('based on our conversation')) {
      console.log('✓ Found existing fallback summary with keywords');
      return true;
    }
  }
  
  console.log('✗ No existing fallback summary found in conversation');
  return false;
}

function fallbackGenerateSummary(messages, participantId) {
  try {
    // Load participant data for context
    const participantFile = path.join(participantsDir, `${participantId}.json`);
    const participant = readJson(participantFile);
    
    // Generate a basic summary from participant messages
    const userMessages = messages.filter(msg => msg.role === 'user' &&
      msg.content &&
      msg.content.trim().length > 10 &&
      !msg.content.toLowerCase().includes('end the chat'));
    
    const summaryPoints = [];
    
    // Add profile-based summary point if available
    if (participant?.belief_change?.mind_change_direction) {
      const direction = participant.belief_change.mind_change_direction;
      const otherText = participant.belief_change.mind_change_other_text;
      
      let changeDesc = '';
      switch (direction) {
        case 'exists_to_not_exists':
          changeDesc = 'From thinking climate change exists, to thinking it does not exist';
          break;
        case 'not_exists_to_exists':
          changeDesc = 'From thinking climate change does not exist, to thinking it exists';
          break;
        case 'not_urgent_to_urgent':
          changeDesc = 'From thinking climate change is not urgent, to thinking it is urgent';
          break;
        case 'urgent_to_not_urgent':
          changeDesc = 'From thinking climate change is urgent, to thinking it is not urgent';
          break;
        case 'human_to_natural':
          changeDesc = 'From thinking climate change is human-caused, to thinking it is natural';
          break;
        case 'natural_to_human':
          changeDesc = 'From thinking climate change is natural, to thinking it is human-caused';
          break;
        case 'other':
          changeDesc = otherText || 'Other belief change described by participant';
          break;
      }
      
      if (changeDesc) {
        summaryPoints.push(`You described your belief change: ${changeDesc}`);
      }
    }
    
    // Analyze user messages for key themes
    const themes = {
      evidence: false,
      personal: false,
      social: false,
      media: false,
      change_process: false
    };
    
    userMessages.forEach(msg => {
      const content = msg.content.toLowerCase();
      
      if (content.includes('evidence') || content.includes('research') || content.includes('study') || content.includes('data')) {
        themes.evidence = true;
      }
      
      if (content.includes('experience') || content.includes('personal') || content.includes('saw') || content.includes('noticed') || content.includes('felt')) {
        themes.personal = true;
      }
      
      if (content.includes('people') || content.includes('family') || content.includes('friend') || content.includes('others')) {
        themes.social = true;
      }
      
      if (content.includes('media') || content.includes('news') || content.includes('article') || content.includes('tv')) {
        themes.media = true;
      }
      
      if (content.includes('change') || content.includes('shift') || content.includes('different') || content.includes('realized')) {
        themes.change_process = true;
      }
    });
    
    // Generate theme-based summary points
    if (themes.evidence) {
      summaryPoints.push('You discussed the role of evidence and research in shaping your views');
    }
    
    if (themes.personal) {
      summaryPoints.push('You shared personal experiences that influenced your thinking');
    }
    
    if (themes.social) {
      summaryPoints.push('You talked about how other people influenced your perspective');
    }
    
    if (themes.media) {
      summaryPoints.push('You mentioned media sources that affected your views');
    }
    
    if (themes.change_process) {
      summaryPoints.push('You described the process of how your beliefs evolved');
    }
    
    // Ensure we have at least 2 points
    if (summaryPoints.length < 2) {
      summaryPoints.push('You engaged in a conversation about your climate change belief journey');
      if (summaryPoints.length < 2) {
        summaryPoints.push('You shared your perspective on what influences belief change');
      }
    }
    
    // Limit to 5 points max
    const finalPoints = summaryPoints.slice(0, 5);
    
    console.log('Generated fallback summary with points:', finalPoints);
    return finalPoints;
    
  } catch (error) {
    console.error('Error generating fallback summary:', error);
    return ['You participated in a conversation about climate change beliefs'];
  }
}

async function fallbackEnsureConversationSummary(conversationId, participantId) {
  try {
    const filename = path.join(conversationsDir, `${conversationId}.json`);
    const conversationData = readJson(filename);
    
    if (!conversationData) {
      console.error('Could not load conversation data for summary generation');
      return;
    }
    
    const messages = conversationData.messages || [];
    
    // Check if conversation already has a proper summary
    if (fallbackHasExistingSummary(messages)) {
      console.log('Fallback conversation already has summary, no action needed');
      return;
    }
    
    // Generate fallback summary
    const summaryPoints = fallbackGenerateSummary(messages, participantId);
    
    // Format as bullet points with proper spacing
    const summaryText = `Thank you for sharing your story with me. Let me summarize the key themes from our conversation:

${summaryPoints.map(point => `• ${point}`).join('\n\n')}

This covers the main points we discussed about your belief change journey.`;

    // Add the summary as a final assistant message
    const summaryMessage = {
      role: "assistant",
      content: summaryText,
      timestamp: new Date().toISOString(),
      generated_summary: true // Flag to indicate this was auto-generated
    };
    
    conversationData.messages.push(summaryMessage);
    
    // Save updated conversation using data access layer
    await dataAccess.saveSession(conversationData);
    
    console.log('✓ Generated and saved fallback summary for conversation:', conversationId);
    
  } catch (error) {
    console.error('Error ensuring fallback conversation summary:', error);
    // Don't throw - this is a safety net, not critical path
  }
}

// Initialize OpenAI client (works with OpenRouter too)
const openai = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY,
    baseURL: process.env.OPENAI_API_KEY?.startsWith('sk-or-')
        ? 'https://openrouter.ai/api/v1'
        : 'https://api.openai.com/v1'
});

// Real OpenAI API integration with timeout handling
async function generateAIResponse(messages, systemPrompt) {
    const API_TIMEOUT = 25000; // 25 seconds - well under typical PaaS 30s timeout
    
    try {
        // Check if OpenAI API key is configured
        if (!process.env.OPENAI_API_KEY) {
            console.error('OpenAI API key not configured. Using fallback response.');
            return "I'm here to help you explore your thoughts about climate change. Could you tell me more about your perspective?";
        }

        // Convert conversation messages to OpenAI format
        const openaiMessages = [
            {
                role: "system",
                content: systemPrompt
            }
        ];

        // Add conversation history
        for (const msg of messages) {
            if (msg.role === 'user' || msg.role === 'assistant') {
                openaiMessages.push({
                    role: msg.role,
                    content: msg.content
                });
            }
        }

        console.log('Sending request to OpenAI with', openaiMessages.length, 'messages');
        const startTime = Date.now();

        // Create timeout promise
        const timeoutPromise = new Promise((_, reject) =>
            setTimeout(() => reject(new Error('OpenAI API timeout')), API_TIMEOUT)
        );

        // Call OpenAI API with timeout
        const apiPromise = openai.chat.completions.create({
            model: "gpt-4o-mini",
            messages: openaiMessages,
            max_tokens: 150,
            temperature: 0.7,
            top_p: 1,
            frequency_penalty: 0,
            presence_penalty: 0
        });

        const completion = await Promise.race([apiPromise, timeoutPromise]);
        const duration = Date.now() - startTime;

        const response = completion.choices[0]?.message?.content?.trim();
        
        if (!response) {
            throw new Error('No response received from OpenAI');
        }

        console.log(`OpenAI response received (${duration}ms):`, response.substring(0, 100) + '...');
        return response;

    } catch (error) {
        const duration = Date.now() - (Date.now() - API_TIMEOUT);
        console.error(`Error calling OpenAI API (${duration}ms):`, error.message);
        
        // Specific error handling for timeouts and connection issues
        if (error.message.includes('timeout') || error.message.includes('ECONNRESET') || error.message.includes('ENOTFOUND')) {
            console.warn('OpenAI API connection issue detected, using fallback');
        }
        
        // Intelligent fallback response system
        console.log('Using fallback response due to OpenAI error');
        return generateIntelligentFallback(messages);
    }
}

// Intelligent fallback response generator
function generateIntelligentFallback(messages) {
    // Get the last user message
    const lastUserMessage = messages.filter(msg => msg.role === 'user').pop();
    const userInput = lastUserMessage ? lastUserMessage.content.toLowerCase().trim() : '';
    
    // Check for gibberish/nonsensical input
    if (isGibberish(userInput)) {
        return "I'd like to understand your perspective better. Could you share your thoughts about climate change in a way that helps me follow along?";
    }
    
    // Check for conversation control words
    if (isConversationControl(userInput)) {
        return "No problem at all. Is there anything else about climate change you'd like to explore or discuss?";
    }
    
    // Check for very short responses
    if (userInput.length < 10) {
        return "I'd love to hear more about your thoughts. Could you elaborate on your perspective about climate change?";
    }
    
    // Check conversation length to vary responses
    const conversationLength = messages.filter(msg => msg.role === 'user').length;
    
    if (conversationLength === 1) {
        // First response - welcoming
        return "To start: could you describe what led you to change your mind about climate change?";
    } else if (conversationLength <= 3) {
        // Early conversation - exploring
        const earlyResponses = [
            "That's helpful context. What specific experiences or information were most influential in shaping that view?",
            "I can see this is something you've thought about. Could you tell me more about what factors were most important to you?",
            "Thank you for sharing that perspective. Are there particular aspects of this issue that you find most compelling?"
        ];
        return earlyResponses[Math.floor(Math.random() * earlyResponses.length)];
    } else {
        // Later conversation - deeper exploration
        const laterResponses = [
            "That's a thoughtful point. How do you think others who disagree might respond to that argument?",
            "I appreciate you explaining your viewpoint. What questions do you think are most important to consider about this issue?",
            "That's interesting. How has your thinking evolved as you've learned more about this topic?",
            "Thank you for that insight. What would you say to someone who holds the opposite view?"
        ];
        return laterResponses[Math.floor(Math.random() * laterResponses.length)];
    }
}

// Helper function to detect gibberish input
function isGibberish(input) {
    // Check for very short random strings
    if (input.length < 5 && !/\b(yes|no|maybe|ok|sure)\b/.test(input)) {
        return true;
    }
    
    // Check for repeated characters (like "aaaaa" or "ededed")
    if (/(.)\1{3,}/.test(input) || /(.{1,3})\1{2,}/.test(input)) {
        return true;
    }
    
    // Check for lack of vowels (excluding common abbreviations)
    const consonantOnly = /^[bcdfghjklmnpqrstvwxyz\s]+$/i.test(input) && input.length > 3;
    if (consonantOnly) {
        return true;
    }
    
    // Check for random keysmashing patterns
    const keysmash = /^[qwertyuiopasdfghjklzxcvbnm]{4,}$/i.test(input.replace(/\s/g, ''));
    if (keysmash && !/\b(real|word|here|there|when|what|where|why|how)\b/i.test(input)) {
        return true;
    }
    
    return false;
}

// Helper function to detect conversation control messages
function isConversationControl(input) {
    const controlWords = [
        'nevermind', 'never mind', 'forget it', 'skip', 'next', 'move on',
        'stop', 'quit', 'end', 'done', 'finished', 'exit', 'bye', 'goodbye'
    ];
    
    return controlWords.some(word => input.includes(word));
}

// Enhanced server startup with database initialization
async function startServer() {
    const PORT = process.env.PORT || 3000;
    
    try {
        // Step 1: Initialize database schema
        console.log('🗃️ Initializing database...');
        const dbInitialized = await database.initializeDatabase();
        
        // Step 2: Test database connection
        const dbAvailable = await database.isDatabaseAvailable();
        if (dbAvailable) {
            console.log('✅ Database connection verified');
            
            // Get database statistics
            const stats = await database.getDatabaseStats();
            if (stats) {
                console.log(`📊 Database stats: ${stats.tables.participants.count} participants, ${stats.tables.sessions.count} sessions, ${stats.tables.messages.count} messages`);
            }
        } else {
            console.log('⚠️ Database unavailable - using file storage fallback');
        }
        
        // Step 3: Start Express server
        const server = app.listen(PORT, () => {
            console.log(`🚀 Server running on http://localhost:${PORT}`);
            console.log('✅ Health endpoint mounted at GET /health');
            console.log('✅ Database stats endpoint mounted at GET /api/database-stats');
            console.log('✅ Database export endpoint mounted at GET /export/database');
            
            if (process.env.NODE_ENV !== 'production') {
                console.log('🔧 Debug endpoints available (development only)');
                console.log('📝 Development logging enabled');
            }
        });
        
        // Step 4: Setup graceful shutdown
        const gracefulShutdown = async (signal) => {
            console.log(`\n📤 Received ${signal}, shutting down gracefully...`);
            
            // Close Express server
            server.close(() => {
                console.log('✅ Express server closed');
            });
            
            // Close database connections
            try {
                await database.closeDatabase();
                console.log('✅ Database connections closed');
            } catch (error) {
                console.error('❌ Error closing database:', error.message);
            }
            
            // Exit process
            process.exit(0);
        };
        
        // Handle shutdown signals
        process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
        process.on('SIGINT', () => gracefulShutdown('SIGINT'));
        
    } catch (error) {
        console.error('❌ Server startup failed:', error);
        process.exit(1);
    }
}

// Start the server
startServer();
