const fs = require('fs');
const path = require('path');

const TARGET_DB = process.env.TARGET_DB || 'customer_campaign_analytics';
const DATA_DIR = process.env.DATA_DIR || path.join('.', 'data', 'processed');

const FILES = {
  campaigns: process.env.CAMPAIGNS_FILE || path.join(DATA_DIR, 'campaigns.csv'),
  clients: process.env.CLIENTS_FILE || path.join(DATA_DIR, 'client_first_purchase_date.csv'),
  events: process.env.EVENTS_FILE || path.join(DATA_DIR, 'events.csv'),
  messages: process.env.MESSAGES_FILE || path.join(DATA_DIR, 'messages.csv'),
  friends: process.env.FRIENDS_FILE || path.join(DATA_DIR, 'friends.csv')
};

const COLLECTIONS = [
  'users',
  'clients',
  'products',
  'events',
  'campaigns',
  'messages',
  'friendships'
];

const BATCH_SIZE = 5000;

function resolvePath(p) {
  return path.isAbsolute(p) ? p : path.resolve(process.cwd(), p);
}

function normalizeString(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const trimmed = String(value).trim();
  return trimmed === '' ? null : trimmed;
}

function parseBoolean(value) {
  const normalized = normalizeString(value);
  if (normalized === null) {
    return null;
  }
  const lower = normalized.toLowerCase();
  if (['1', 't', 'true', 'yes', 'y'].includes(lower)) {
    return true;
  }
  if (['0', 'f', 'false', 'no', 'n'].includes(lower)) {
    return false;
  }
  return null;
}

function parseDate(value) {
  const normalized = normalizeString(value);
  if (normalized === null) {
    return null;
  }

  let iso;
  if (/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
    iso = `${normalized}T00:00:00Z`;
  } else {
    iso = normalized.replace(' ', 'T');
    if (!/[zZ]$|[+-]\d{2}:?\d{2}$/.test(iso)) {
      iso = `${iso}Z`;
    }
  }

  const parsed = new Date(iso);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function parseDecimal(value) {
  const normalized = normalizeString(value);
  return normalized === null ? null : NumberDecimal(normalized);
}

function parseLong(value) {
  const normalized = normalizeString(value);
  return normalized === null ? null : NumberLong(normalized);
}

function parseCsvLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];

    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      result.push(current);
      current = '';
    } else {
      current += ch;
    }
  }

  result.push(current);
  return result;
}

function isLikelyGitLfsPointer(filePath) {
  const fd = fs.openSync(filePath, 'r');

  try {
    const sampleBuffer = Buffer.alloc(256);
    const bytesRead = fs.readSync(fd, sampleBuffer, 0, sampleBuffer.length, 0);
    const sample = sampleBuffer.toString('utf8', 0, bytesRead);
    return sample.startsWith('version https://git-lfs.github.com/spec/v1');
  } finally {
    fs.closeSync(fd);
  }
}

async function readCsv(filePath, onRow) {
  const absolute = resolvePath(filePath);
  if (!fs.existsSync(absolute)) {
    throw new Error(`CSV file not found: ${absolute}`);
  }
  if (isLikelyGitLfsPointer(absolute)) {
    throw new Error(
      `CSV file is a Git LFS pointer, not the actual dataset: ${absolute}. Run "git lfs pull" to fetch the real file contents.`
    );
  }

  let header = null;
  let lineNo = 0;
  let buffer = '';

  async function processLine(rawLine) {
    lineNo += 1;
    const line = rawLine.replace(/\r$/, '');

    if (lineNo === 1) {
      header = parseCsvLine(line);
      return;
    }

    if (!line.trim()) {
      return;
    }

    const values = parseCsvLine(line);
    if (values.length < header.length) {
      while (values.length < header.length) {
        values.push('');
      }
    }

    const row = {};
    for (let i = 0; i < header.length; i += 1) {
      row[header[i]] = values[i];
    }

    await onRow(row, lineNo);
  }

  const stream = fs.createReadStream(absolute, { encoding: 'utf8' });

  try {
    for await (const chunk of stream) {
      buffer += chunk;

      let newlineIndex = buffer.indexOf('\n');
      while (newlineIndex !== -1) {
        const rawLine = buffer.slice(0, newlineIndex);
        buffer = buffer.slice(newlineIndex + 1);
        await processLine(rawLine);
        newlineIndex = buffer.indexOf('\n');
      }
    }

    if (buffer.length > 0) {
      await processLine(buffer);
    }
  } finally {
    stream.destroy();
  }
}

function compareNullableAsc(a, b) {
  if (a === null && b === null) return 0;
  if (a === null) return 1;
  if (b === null) return -1;
  return a < b ? -1 : a > b ? 1 : 0;
}

function compareNullableDateAsc(a, b) {
  if (a === null && b === null) return 0;
  if (a === null) return 1;
  if (b === null) return -1;
  const ta = a.getTime();
  const tb = b.getTime();
  return ta < tb ? -1 : ta > tb ? 1 : 0;
}

function betterClientCandidate(candidate, current) {
  if (!current) {
    return true;
  }

  const firstPurchaseCmp = compareNullableDateAsc(candidate.first_purchase_date, current.first_purchase_date);
  if (firstPurchaseCmp !== 0) {
    return firstPurchaseCmp < 0;
  }

  const userCmp = compareNullableAsc(candidate.user_id, current.user_id);
  if (userCmp !== 0) {
    return userCmp < 0;
  }

  return compareNullableAsc(candidate.user_device_id, current.user_device_id) < 0;
}

function betterMessageClientCandidate(candidate, current) {
  if (!current) {
    return true;
  }

  const userCmp = compareNullableAsc(candidate.user_id, current.user_id);
  if (userCmp !== 0) {
    return userCmp < 0;
  }

  return compareNullableAsc(candidate.user_device_id, current.user_device_id) < 0;
}

function betterProductCandidate(candidate, current) {
  if (!current) {
    return true;
  }

  const categoryIdCmp = compareNullableAsc(candidate.category_id, current.category_id);
  if (categoryIdCmp !== 0) {
    return categoryIdCmp < 0;
  }

  const categoryCodeCmp = compareNullableAsc(candidate.category_code, current.category_code);
  if (categoryCodeCmp !== 0) {
    return categoryCodeCmp < 0;
  }

  return compareNullableAsc(candidate.brand, current.brand) < 0;
}

function betterCampaignCandidate(candidate, current) {
  if (!current) {
    return true;
  }

  const candFinished = candidate.finished_at;
  const currFinished = current.finished_at;

  if (candFinished !== null || currFinished !== null) {
    if (candFinished === null) return false;
    if (currFinished === null) return true;
    if (candFinished.getTime() !== currFinished.getTime()) {
      return candFinished.getTime() > currFinished.getTime();
    }
  }

  const candStarted = candidate.started_at;
  const currStarted = current.started_at;

  if (candStarted !== null || currStarted !== null) {
    if (candStarted === null) return false;
    if (currStarted === null) return true;
    if (candStarted.getTime() !== currStarted.getTime()) {
      return candStarted.getTime() > currStarted.getTime();
    }
  }

  return false;
}

function buildProductSearchText(categoryCode, brand) {
  const parts = [categoryCode, brand].filter((v) => v !== null);
  if (parts.length === 0) {
    return null;
  }
  return parts.join(' ').toLowerCase();
}

function createCollections(dbRef) {
  for (const collectionName of COLLECTIONS) {
    if (dbRef.getCollectionNames().includes(collectionName)) {
      dbRef.getCollection(collectionName).drop();
    }
  }

  dbRef.createCollection('users', {
    capped: false,
    validator: {
      $jsonSchema: {
        bsonType: 'object',
        title: 'users',
        properties: {
          _id: { bsonType: 'string' }
        },
        additionalProperties: false
      }
    },
    validationLevel: 'moderate',
    validationAction: 'warn'
  });

  dbRef.createCollection('clients', {
    capped: false,
    validator: {
      $jsonSchema: {
        bsonType: 'object',
        title: 'clients',
        properties: {
          _id: { bsonType: 'string' },
          user_id: { bsonType: 'string' },
          user_device_id: { bsonType: 'string' },
          first_purchase_date: { bsonType: ['null', 'date'] }
        },
        additionalProperties: false
      }
    },
    validationLevel: 'moderate',
    validationAction: 'warn'
  });

  dbRef.clients.createIndex({ first_purchase_date: 1 }, { name: 'idx_purchases' });
  dbRef.clients.createIndex({ user_id: 1 }, { name: 'idx_user_id' });

  dbRef.createCollection('products', {
    capped: false,
    validator: {
      $jsonSchema: {
        bsonType: 'object',
        title: 'products',
        properties: {
          _id: { bsonType: 'string' },
          category_id: { bsonType: ['string', 'null'] },
          category_code: { bsonType: ['string', 'null'] },
          category_search_text: { bsonType: ['string', 'null'] },
          brand: { bsonType: ['string', 'null'] }
        },
        additionalProperties: false
      }
    },
    validationLevel: 'moderate',
    validationAction: 'warn'
  });

  dbRef.createCollection('events', {
    capped: false,
    validator: {
      $jsonSchema: {
        bsonType: 'object',
        title: 'events',
        properties: {
          _id: { bsonType: 'string' },
          event_time: { bsonType: 'date' },
          event_type: { bsonType: 'string' },
          user_id: { bsonType: 'string' },
          product_id: { bsonType: 'string' },
          category_id: { bsonType: ['string', 'null'] },
          category_code: { bsonType: ['string', 'null'] },
          brand: { bsonType: ['string', 'null'] },
          price: { bsonType: 'decimal' },
          user_session: { bsonType: 'string' }
        },
        additionalProperties: false
      }
    },
    validationLevel: 'moderate',
    validationAction: 'warn'
  });

  dbRef.events.createIndex({ user_id: 1 }, { name: 'idx_user_id' });
  dbRef.events.createIndex({ product_id: 1 }, { name: 'idx_product_id' });

  dbRef.createCollection('campaigns', {
    capped: false,
    validator: {
      $jsonSchema: {
        bsonType: 'object',
        title: 'campaigns',
        properties: {
          _id: { bsonType: 'string' },
          campaign_id: { bsonType: 'string' },
          campaign_type: { bsonType: 'string' },
          channel: { bsonType: 'string' },
          topic: { bsonType: ['string', 'null'] },
          started_at: { bsonType: ['date', 'null'] },
          finished_at: { bsonType: ['date', 'null'] },
          total_count: { bsonType: ['long', 'null'] },
          ab_test: { bsonType: ['bool', 'null'] },
          warmup_mode: { bsonType: ['bool', 'null'] },
          hour_limit: { bsonType: ['decimal', 'null'] },
          subject_length: { bsonType: ['decimal', 'null'] },
          is_test: { bsonType: ['bool', 'null'] },
          position: { bsonType: ['long', 'null'] },
          subject_with_personalization: { bsonType: ['bool', 'null'] },
          subject_with_deadline: { bsonType: ['bool', 'null'] },
          subject_with_emoji: { bsonType: ['bool', 'null'] },
          subject_with_bonuses: { bsonType: ['bool', 'null'] },
          subject_with_discount: { bsonType: ['bool', 'null'] },
          subject_with_saleout: { bsonType: ['bool', 'null'] }
        },
        additionalProperties: false
      }
    },
    validationLevel: 'moderate',
    validationAction: 'warn'
  });

  dbRef.campaigns.createIndex({ campaign_id: 1 }, { name: 'idx_campaign_id' });
  dbRef.campaigns.createIndex({ campaign_type: 1 }, { name: 'campaign_type' });
  dbRef.campaigns.createIndex({ campaign_type: 1, campaign_id: 1 }, { name: 'idx_campaign_type_id' });
  dbRef.campaigns.createIndex({ channel: 1 }, { name: 'idx_channel' });
  dbRef.campaigns.createIndex({ topic: 1 }, { name: 'idx_topic' });

  dbRef.createCollection('messages', {
    capped: false,
    validator: {
      $jsonSchema: {
        bsonType: 'object',
        title: 'messages',
        properties: {
          _id: { bsonType: 'string' },
          message_id: { bsonType: 'string' },
          campaign_ref: {
            bsonType: 'object',
            properties: {
              campaign_id: { bsonType: ['string', 'null'] },
              campaign_type: { bsonType: ['string', 'null'] },
              campaign_key: { bsonType: ['string', 'null'] }
            },
            additionalProperties: false
          },
          client_id: { bsonType: 'string' },
          user_id: { bsonType: 'string' },
          channel: { bsonType: 'string' },
          category: { bsonType: ['string', 'null'] },
          platform: { bsonType: ['string', 'null'] },
          email_provider: { bsonType: ['string', 'null'] },
          stream: { bsonType: ['string', 'null'] },
          date: { bsonType: 'date' },
          sent_at: { bsonType: 'date' },
          engagement: {
            bsonType: 'object',
            properties: {
              is_opened: { bsonType: 'bool' },
              opened_first_time_at: { bsonType: ['date', 'null'] },
              opened_last_time_at: { bsonType: ['date', 'null'] },
              is_clicked: { bsonType: 'bool' },
              clicked_first_time_at: { bsonType: ['date', 'null'] },
              clicked_last_time_at: { bsonType: ['date', 'null'] },
              is_unsubscribed: { bsonType: 'bool' },
              is_hard_bounced: { bsonType: 'bool' },
              hard_bounced_at: { bsonType: ['date', 'null'] },
              is_soft_bounced: { bsonType: 'bool' },
              soft_bounced_at: { bsonType: ['date', 'null'] },
              is_complained: { bsonType: 'bool' },
              complained_at: { bsonType: ['date', 'null'] },
              is_blocked: { bsonType: 'bool' },
              blocked_at: { bsonType: ['date', 'null'] },
              is_purchased: { bsonType: 'bool' },
              purchased_at: { bsonType: ['date', 'null'] },
              unsubscribed_at: { bsonType: ['date', 'null'] }
            },
            additionalProperties: false
          },
          created_at: { bsonType: 'date' },
          updated_at: { bsonType: 'date' },
          user_device_id: { bsonType: 'string' }
        },
        additionalProperties: false
      }
    },
    validationLevel: 'moderate',
    validationAction: 'warn'
  });

  dbRef.messages.createIndex({ message_id: 1 }, { name: 'idx_message_id' });
  dbRef.messages.createIndex({ client_id: 1 }, { name: 'idx_client_id' });
  dbRef.messages.createIndex({ user_id: 1 }, { name: 'idx_user_id' });
  dbRef.messages.createIndex({ 'campaign_ref.campaign_key': 1 }, { name: 'idx_campaign_ref' });
  dbRef.messages.createIndex({ channel: 1 }, { name: 'idx_channel' });
  dbRef.messages.createIndex({ sent_at: 1 }, { name: 'sent_at' });
  dbRef.messages.createIndex({ client_id: 1, sent_at: 1 }, { name: 'idx_client_id_sent_at' });
  dbRef.messages.createIndex({ user_id: 1, sent_at: 1 }, { name: 'idx_user_is_sent_at' });
  dbRef.messages.createIndex({ 'campaign_ref.campaign_key': 1, sent_at: 1 }, { name: 'idx_campaign_key_sent_at' });

  dbRef.createCollection('friendships', {
    capped: false,
    validator: {
      $jsonSchema: {
        bsonType: 'object',
        title: 'friendships',
        properties: {
          _id: { bsonType: 'string' },
          user_id: { bsonType: 'string' },
          friend_id: { bsonType: 'string' }
        },
        additionalProperties: false
      }
    },
    validationLevel: 'moderate',
    validationAction: 'warn'
  });

  dbRef.friendships.createIndex({ user_id: 1 }, { name: 'idx_user_id' });
  dbRef.friendships.createIndex({ friend_id: 1 }, { name: 'idx_friend_id' });
}

async function insertBatched(collection, docs) {
  if (docs.length === 0) {
    return;
  }
  await collection.insertMany(docs, { ordered: false });
  docs.length = 0;
}

async function main() {
  print(`Using database: ${TARGET_DB}`);
  print(`Using data directory: ${resolvePath(DATA_DIR)}`);

  for (const [name, p] of Object.entries(FILES)) {
    const resolved = resolvePath(p);
    if (!fs.existsSync(resolved)) {
      throw new Error(`Missing ${name} CSV file at ${resolved}`);
    }
  }

  const dbRef = db.getSiblingDB(TARGET_DB);
  createCollections(dbRef);

  const users = new Set();
  const clients = new Map();
  const messageClients = new Map();
  const products = new Map();
  const campaigns = new Map();
  const friendshipKeys = new Set();

  print('Reading campaigns.csv...');
  await readCsv(FILES.campaigns, async (row) => {
    const campaignId = normalizeString(row.id);
    const campaignType = normalizeString(row.campaign_type);

    if (campaignId === null || campaignType === null) {
      return;
    }

    const key = `${campaignId}|${campaignType}`;
    const candidate = {
      _id: key,
      campaign_id: campaignId,
      campaign_type: campaignType,
      channel: normalizeString(row.channel),
      topic: normalizeString(row.topic),
      started_at: parseDate(row.started_at),
      finished_at: parseDate(row.finished_at),
      total_count: parseLong(row.total_count),
      ab_test: parseBoolean(row.ab_test),
      warmup_mode: parseBoolean(row.warmup_mode),
      hour_limit: parseDecimal(row.hour_limit),
      subject_length: parseDecimal(row.subject_length),
      subject_with_personalization: parseBoolean(row.subject_with_personalization),
      subject_with_deadline: parseBoolean(row.subject_with_deadline),
      subject_with_emoji: parseBoolean(row.subject_with_emoji),
      subject_with_bonuses: parseBoolean(row.subject_with_bonuses),
      subject_with_discount: parseBoolean(row.subject_with_discount),
      subject_with_saleout: parseBoolean(row.subject_with_saleout),
      is_test: parseBoolean(row.is_test),
      position: parseLong(row.position)
    };

    const current = campaigns.get(key);
    if (betterCampaignCandidate(candidate, current)) {
      campaigns.set(key, candidate);
    }
  });

  print('Reading client_first_purchase_date.csv...');
  await readCsv(FILES.clients, async (row) => {
    const clientId = normalizeString(row.client_id);
    if (clientId === null) {
      return;
    }

    const candidate = {
      _id: clientId,
      user_id: normalizeString(row.user_id),
      user_device_id: normalizeString(row.user_device_id),
      first_purchase_date: parseDate(row.first_purchase_date)
    };

    if (candidate.user_id !== null) {
      users.add(candidate.user_id);
    }

    const current = clients.get(clientId);
    if (betterClientCandidate(candidate, current)) {
      clients.set(clientId, candidate);
    }
  });

  print('Reading events.csv and loading events...');
  const eventBatch = [];
  let eventIdSeq = 0;

  await readCsv(FILES.events, async (row) => {
    const userId = normalizeString(row.user_id);
    const productId = normalizeString(row.product_id);

    if (userId === null || productId === null) {
      return;
    }

    users.add(userId);

    const categoryId = normalizeString(row.category_id);
    const categoryCode = normalizeString(row.category_code);
    const brand = normalizeString(row.brand);

    const productCandidate = {
      _id: productId,
      category_id: categoryId,
      category_code: categoryCode,
      category_search_text: buildProductSearchText(categoryCode, brand),
      brand
    };

    const currentProduct = products.get(productId);
    if (betterProductCandidate(productCandidate, currentProduct)) {
      products.set(productId, productCandidate);
    }

    eventIdSeq += 1;
    eventBatch.push({
      _id: `ev_${eventIdSeq}`,
      event_time: parseDate(row.event_time),
      event_type: normalizeString(row.event_type),
      user_id: userId,
      product_id: productId,
      category_id: categoryId,
      category_code: categoryCode,
      brand,
      price: parseDecimal(row.price),
      user_session: normalizeString(row.user_session)
    });

    if (eventBatch.length >= BATCH_SIZE) {
      await insertBatched(dbRef.events, eventBatch);
    }
  });
  await insertBatched(dbRef.events, eventBatch);

  print('Reading messages.csv and loading messages...');
  const messageBatch = [];

  await readCsv(FILES.messages, async (row) => {
    const id = normalizeString(row.id);
    const clientId = normalizeString(row.client_id);
    const userId = normalizeString(row.user_id);
    const campaignId = normalizeString(row.campaign_id);
    const messageType = normalizeString(row.message_type);

    if (id === null || clientId === null || userId === null || campaignId === null || messageType === null) {
      return;
    }

    users.add(userId);

    const msgClientCandidate = {
      _id: clientId,
      user_id: userId,
      user_device_id: normalizeString(row.user_device_id),
      first_purchase_date: null
    };
    const currentMessageClient = messageClients.get(clientId);
    if (betterMessageClientCandidate(msgClientCandidate, currentMessageClient)) {
      messageClients.set(clientId, msgClientCandidate);
    }

    const campaignKey = `${campaignId}|${messageType}`;

    messageBatch.push({
      _id: id,
      message_id: normalizeString(row.message_id),
      campaign_ref: {
        campaign_id: campaignId,
        campaign_type: messageType,
        campaign_key: campaignKey
      },
      client_id: clientId,
      user_id: userId,
      channel: normalizeString(row.channel),
      category: normalizeString(row.category),
      platform: normalizeString(row.platform),
      email_provider: normalizeString(row.email_provider),
      stream: normalizeString(row.stream),
      date: parseDate(row.date),
      sent_at: parseDate(row.sent_at),
      engagement: {
        is_opened: parseBoolean(row.is_opened),
        opened_first_time_at: parseDate(row.opened_first_time_at),
        opened_last_time_at: parseDate(row.opened_last_time_at),
        is_clicked: parseBoolean(row.is_clicked),
        clicked_first_time_at: parseDate(row.clicked_first_time_at),
        clicked_last_time_at: parseDate(row.clicked_last_time_at),
        is_unsubscribed: parseBoolean(row.is_unsubscribed),
        unsubscribed_at: parseDate(row.unsubscribed_at),
        is_hard_bounced: parseBoolean(row.is_hard_bounced),
        hard_bounced_at: parseDate(row.hard_bounced_at),
        is_soft_bounced: parseBoolean(row.is_soft_bounced),
        soft_bounced_at: parseDate(row.soft_bounced_at),
        is_complained: parseBoolean(row.is_complained),
        complained_at: parseDate(row.complained_at),
        is_blocked: parseBoolean(row.is_blocked),
        blocked_at: parseDate(row.blocked_at),
        is_purchased: parseBoolean(row.is_purchased),
        purchased_at: parseDate(row.purchased_at)
      },
      created_at: parseDate(row.created_at),
      updated_at: parseDate(row.updated_at),
      user_device_id: normalizeString(row.user_device_id)
    });

    if (messageBatch.length >= BATCH_SIZE) {
      await insertBatched(dbRef.messages, messageBatch);
    }
  });
  await insertBatched(dbRef.messages, messageBatch);

  print('Reading friends.csv and loading friendships...');
  const friendshipBatch = [];

  await readCsv(FILES.friends, async (row) => {
    const userId = normalizeString(row.friend1);
    const friendId = normalizeString(row.friend2);

    if (userId === null || friendId === null || userId === friendId) {
      return;
    }

    users.add(userId);
    users.add(friendId);

    const key = `${userId}|${friendId}`;
    if (friendshipKeys.has(key)) {
      return;
    }
    friendshipKeys.add(key);

    friendshipBatch.push({
      _id: key,
      user_id: userId,
      friend_id: friendId
    });

    if (friendshipBatch.length >= BATCH_SIZE) {
      await insertBatched(dbRef.friendships, friendshipBatch);
    }
  });
  await insertBatched(dbRef.friendships, friendshipBatch);

  for (const [clientId, candidate] of messageClients.entries()) {
    if (!clients.has(clientId)) {
      clients.set(clientId, candidate);
    }
  }

  print('Loading campaigns collection...');
  const campaignDocs = Array.from(campaigns.values());
  for (let i = 0; i < campaignDocs.length; i += BATCH_SIZE) {
    await dbRef.campaigns.insertMany(campaignDocs.slice(i, i + BATCH_SIZE), { ordered: false });
  }

  print('Loading clients collection...');
  const clientDocs = Array.from(clients.values());
  for (let i = 0; i < clientDocs.length; i += BATCH_SIZE) {
    await dbRef.clients.insertMany(clientDocs.slice(i, i + BATCH_SIZE), { ordered: false });
  }

  print('Loading products collection...');
  const productDocs = Array.from(products.values());
  for (let i = 0; i < productDocs.length; i += BATCH_SIZE) {
    await dbRef.products.insertMany(productDocs.slice(i, i + BATCH_SIZE), { ordered: false });
  }

  print('Loading users collection...');
  const userDocs = Array.from(users).map((userId) => ({ _id: userId }));
  for (let i = 0; i < userDocs.length; i += BATCH_SIZE) {
    await dbRef.users.insertMany(userDocs.slice(i, i + BATCH_SIZE), { ordered: false });
  }

  print('Load completed. Document counts:');
  for (const collectionName of COLLECTIONS) {
    print(`  ${collectionName}: ${dbRef.getCollection(collectionName).countDocuments({})}`);
  }
}

main()
  .catch((err) => {
    print(`Load failed: ${err.message}`);
    throw err;
  });
