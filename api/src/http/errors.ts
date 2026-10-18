export class NotFoundError extends Error {
  override readonly name = 'NotFoundError';
}

export class ConflictError extends Error {
  override readonly name = 'ConflictError';
}

export class BadRequestError extends Error {
  override readonly name = 'BadRequestError';
}
