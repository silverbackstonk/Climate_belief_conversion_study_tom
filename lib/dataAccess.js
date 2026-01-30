const { PrismaClient } = require('@prisma/client');
const fs = require('fs');
const path = require('path');

// Initialize Prisma client
let prisma = null;

function initializePrisma() {
  if (!prisma) {
    if (!process.env.DATABASE_URL) {
      if (process.env.NODE_ENV === 'production') {
        throw new Error('❌ DATABASE_URL is required in production. File storage not allowed.');
      }
      console.warn('⚠️ DATABASE_URL not configured, using file fallback (development only)');
      return null;
    }
    
    try {
      prisma = new PrismaClient();
    } catch (error) {
      console.warn('Prisma client initialization failed:', error.message);
      prisma = null;
    }
  }
  return prisma;
}

// Gracefully disconnect Prisma
async function disconnectPrisma() {
  if (prisma) {
    await prisma.$disconnect();
    prisma = null;
  }
}

// Utility functions from existing server.js
function writeJson(filePath, obj) {
  try {
    const dir = path.dirname(filePath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
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

// Data access layer
class DataAccess {
  constructor() {
    this.conversationsDir = path.join(__dirname, '..', 'data', 'conversations');
    this.participantsDir = path.join(__dirname, '..', 'data', 'participants');
    this.prisma = initializePrisma();
  }

  async saveSession(sessionData) {
    const results = { file: false, database: false };
    
    // Always write to file in development, and as backup in production
    if (process.env.NODE_ENV !== 'production' || !this.prisma) {
      const filename = path.join(this.conversationsDir, `${sessionData.id}.json`);
      results.file = writeJson(filename, sessionData);
    }
    
    // Write to database if Prisma is available
    if (this.prisma) {
      try {
        // Parse participant data for IndividualDifferences
        let participantData = null;
        if (sessionData.participantId) {
          const participantFile = path.join(this.participantsDir, `${sessionData.participantId}.json`);
          participantData = readJson(participantFile);
        }
        
        // Upsert Session record
        await this.prisma.session.upsert({
          where: { id: sessionData.id },
          update: {
            participantId: sessionData.participantId,
            startedAt: sessionData.startedAt ? new Date(sessionData.startedAt) : null,
            completedAt: sessionData.endedAt ? new Date(sessionData.endedAt) : null,
            raw: sessionData,
            updatedAt: new Date()
          },
          create: {
            id: sessionData.id,
            participantId: sessionData.participantId,
            startedAt: sessionData.startedAt ? new Date(sessionData.startedAt) : null,
            completedAt: sessionData.endedAt ? new Date(sessionData.endedAt) : null,
            raw: sessionData
          }
        });
        
        // Save Messages
        if (sessionData.messages && sessionData.messages.length > 0) {
          // Delete existing messages for this session (to handle updates)
          await this.prisma.message.deleteMany({
            where: { sessionId: sessionData.id }
          });
          
          // Insert messages
          const messages = sessionData.messages.map((msg, index) => ({
            id: `${sessionData.id}-msg-${index}`,
            sessionId: sessionData.id,
            turn: index,
            role: msg.role,
            content: msg.content,
            timestamp: msg.timestamp ? new Date(msg.timestamp) : new Date()
          }));
          
          await this.prisma.message.createMany({
            data: messages
          });
        }
        
        // Save IndividualDifferences
        if (participantData && sessionData.endedAt) {
          await this.prisma.individualDifferences.upsert({
            where: { sessionId: sessionData.id },
            update: {
              raw: participantData,
              political7: participantData.politicalAffiliation || null,
              confidence0_100: participantData.confidenceLevel || null,
              age: participantData.age || null,
              gender: participantData.gender || null,
              education: participantData.education || null,
              viewsChanged: participantData.viewsChanged || null
            },
            create: {
              sessionId: sessionData.id,
              raw: participantData,
              political7: participantData.politicalAffiliation || null,
              confidence0_100: participantData.confidenceLevel || null,
              age: participantData.age || null,
              gender: participantData.gender || null,
              education: participantData.education || null,
              viewsChanged: participantData.viewsChanged || null
            }
          });
        }
        
        results.database = true;
      } catch (error) {
        console.error('Database save failed:', error);
        results.database = false;
      }
    }
    
    return results;
  }
  
  async getLatestCompletedSession() {
    // Try database first if available
    if (this.prisma) {
      try {
        const session = await this.prisma.session.findFirst({
          where: {
            completedAt: { not: null }
          },
          orderBy: {
            completedAt: 'desc'
          },
          include: {
            messages: {
              orderBy: { turn: 'asc' }
            },
            individualDifferences: true
          }
        });
        
        if (session) {
          return session.raw; // Return the original JSON structure
        }
      } catch (error) {
        console.error('Database query failed, falling back to files:', error);
      }
    }
    
    // Fallback to file system
    try {
      if (!fs.existsSync(this.conversationsDir)) {
        return null;
      }
      
      const conversationFiles = fs.readdirSync(this.conversationsDir)
        .filter(file => file.endsWith('.json'))
        .map(file => {
          const filePath = path.join(this.conversationsDir, file);
          const conversation = readJson(filePath);
          return {
            file,
            conversation,
            completedAt: conversation?.endedAt
          };
        })
        .filter(item => item.conversation && item.completedAt)
        .sort((a, b) => new Date(b.completedAt) - new Date(a.completedAt));
      
      return conversationFiles.length > 0 ? conversationFiles[0].conversation : null;
    } catch (error) {
      console.error('File system fallback failed:', error);
      return null;
    }
  }
  
  async getAllSessions() {
    // Try database first if available
    if (this.prisma) {
      try {
        const sessions = await this.prisma.session.findMany({
          include: {
            messages: {
              orderBy: { turn: 'asc' }
            },
            individualDifferences: true
          },
          orderBy: {
            createdAt: 'desc'
          }
        });
        
        return sessions.map(session => session.raw);
      } catch (error) {
        console.error('Database query failed, falling back to files:', error);
      }
    }
    
    // Fallback to file system
    try {
      if (!fs.existsSync(this.conversationsDir)) {
        return [];
      }
      
      const sessions = fs.readdirSync(this.conversationsDir)
        .filter(file => file.endsWith('.json'))
        .map(file => {
          const filePath = path.join(this.conversationsDir, file);
          return readJson(filePath);
        })
        .filter(session => session);
      
      return sessions;
    } catch (error) {
      console.error('Failed to read sessions:', error);
      return [];
    }
  }
  
  async disconnect() {
    await disconnectPrisma();
  }
}

// Export singleton instance
const dataAccess = new DataAccess();

module.exports = {
  dataAccess,
  initializePrisma,
  disconnectPrisma
};