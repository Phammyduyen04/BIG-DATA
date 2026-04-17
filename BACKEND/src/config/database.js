import { PrismaClient } from '@prisma/client';

let prismaInstance;

/**
 * Initialize Prisma Client
 */
export function initializeDatabase() {
    if (!prismaInstance) {
        prismaInstance = new PrismaClient();
    }
    return prismaInstance;
}

/**
 * Get Prisma Client instance
 */
export function getPrismaClient() {
    if (!prismaInstance) {
        throw new Error('Database client not initialized. Call initializeDatabase first.');
    }
    return prismaInstance;
}

/**
 * Close Prisma Client connection
 */
export async function closeDatabase() {
    if (prismaInstance) {
        await prismaInstance.$disconnect();
    }
}

export default { initializeDatabase, getPrismaClient, closeDatabase };
